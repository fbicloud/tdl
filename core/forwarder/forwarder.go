package forwarder

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/fbicloud/tdl/core/dcpool"
	"github.com/fbicloud/tdl/core/logctx"
	"github.com/fbicloud/tdl/core/tmedia"
	"github.com/fbicloud/tdl/core/util/tutil"
)

//go:generate go-enum --values --names --flag --nocase

// Mode
// ENUM(direct, clone)
type Mode int

type Options struct {
	Pool     dcpool.Pool
	Threads  int
	Iter     Iter
	Progress Progress
}

type Forwarder struct {
	sent map[tuple]struct{} // used to filter grouped messages which are already sent
	mu   sync.Mutex
	rand *rand.Rand
	opts Options
}

type tuple struct {
	from int64
	msg  int
}

func New(opts Options) *Forwarder {
	return &Forwarder{
		sent: make(map[tuple]struct{}),
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		opts: opts,
	}
}

func (f *Forwarder) Forward(ctx context.Context, limit int) error {
	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(limit)

	// flushBatch sends a collected batch of direct-mode messages in one
	// API call.  forwardBatch handles progress callbacks internally.
	flushBatch := func(batch []Elem) {
		if len(batch) == 0 {
			return
		}
		b := make([]Elem, len(batch))
		copy(b, batch)
		wg.Go(func() error {
			if err := f.forwardBatch(wgctx, b); errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		})
	}

	// batch collects consecutive non-grouped direct-mode messages that
	// share the same (from, to, silent, thread, dropAuthor) tuple and
	// are not protected.  Grouped / clone / protected messages are
	// processed immediately and flush any pending batch first.
	var batch []Elem
	var batchKey batchKey
	pendingGroups := make(map[int64]struct{})

	mustFlush := func(next Elem) bool {
		if len(batch) == 0 {
			return false
		}
		if len(batch) >= 100 { // Telegram API limit
			return true
		}
		k := keyOf(next)
		return k != batchKey
	}

	for f.opts.Iter.Next(wgctx) {
		elem := f.opts.Iter.Value()

		// Fetch grouped messages in the main loop so ALL members
		// are marked before the next iterator element is processed.
		// Otherwise the next element (same album) can sneak through
		// before the goroutine finishes GetGroupedMessages.
		var grouped []*tg.Message
		f.mu.Lock()
		if _, ok := f.sent[f.tuple(elem.From(), elem.Msg())]; ok {
			f.mu.Unlock()
			continue
		}
		if _, ok := elem.Msg().GetGroupedID(); ok && elem.AsGrouped() {
			groupID, _ := elem.Msg().GetGroupedID()
			if _, pending := pendingGroups[groupID]; pending {
				f.mu.Unlock()
				continue // another goroutine is handling this group
			}
			pendingGroups[groupID] = struct{}{}
			f.sent[f.tuple(elem.From(), elem.Msg())] = struct{}{}
			f.mu.Unlock()

			// Flush pending batch before spawning grouped goroutine.
			flushBatch(batch)
			batch = batch[:0]

			fromPeer := elem.From().InputPeer()
			wg.Go(func() error {
				defer func() {
					f.mu.Lock()
					delete(pendingGroups, groupID)
					f.mu.Unlock()
				}()

				grouped, err := tutil.GetGroupedMessages(wgctx, f.opts.Pool.Default(wgctx), fromPeer, elem.Msg())
				if err != nil {
					return nil
				}

				f.mu.Lock()
				for _, m := range grouped {
					f.sent[f.tuple(elem.From(), m)] = struct{}{}
				}
				f.mu.Unlock()

				f.opts.Progress.OnAdd(elem)
				if err := f.forwardMessage(wgctx, elem, grouped...); errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			})
			continue
		} else if elem.Mode() == ModeDirect &&
			!protectedDialog(elem.From()) && !protectedMessage(elem.Msg()) {
			// Batchable direct-mode message.
			f.sent[f.tuple(elem.From(), elem.Msg())] = struct{}{}
			f.mu.Unlock()

			if mustFlush(elem) {
				flushBatch(batch)
				batch = batch[:0]
			}
			batch = append(batch, elem)
			if len(batch) == 1 {
				batchKey = keyOf(elem)
			}
			continue
		} else {
			f.sent[f.tuple(elem.From(), elem.Msg())] = struct{}{}
		}
		f.mu.Unlock()

		// Non-batchable: flush pending batch first, then process.
		flushBatch(batch)
		batch = batch[:0]

		wg.Go(func() error {
			f.opts.Progress.OnAdd(elem)

			if len(grouped) > 0 {
				if err := f.forwardMessage(wgctx, elem, grouped...); errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			}

			if err := f.forwardMessage(wgctx, elem); errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		})
	}

	// Flush any remaining batch.
	flushBatch(batch)

	if err := f.opts.Iter.Err(); err != nil {
		return errors.Wrap(err, "iter")
	}

	return wg.Wait()
}

// batchKey identifies the batching group for direct-mode messages.
// Messages with the same key can be forwarded in a single API call.
type batchKey struct {
	from       int64
	to         int64
	silent     bool
	thread     int
	dropAuthor bool
}

func keyOf(e Elem) batchKey {
	return batchKey{
		from:       e.From().ID(),
		to:         e.To().ID(),
		silent:     e.AsSilent(),
		thread:     e.Thread(),
		dropAuthor: e.AsDropAuthor(),
	}
}

// forwardBatch sends multiple direct-mode messages in one
// MessagesForwardMessages call.  All elems must share the same
// (from, to, silent, thread, dropAuthor) tuple and be unprotected.
// Handles its own progress callbacks.  On failure, falls back to
// individual forwardMessage calls.
func (f *Forwarder) forwardBatch(ctx context.Context, elems []Elem) (rerr error) {
	if len(elems) == 0 {
		return nil
	}
	first := elems[0]

	// Track progress for every element in the batch.
	for _, e := range elems {
		f.opts.Progress.OnAdd(e)
	}

	defer func() {
		f.mu.Lock()
		for _, e := range elems {
			f.sent[f.tuple(e.From(), e.Msg())] = struct{}{}
		}
		f.mu.Unlock()
	}()

	ids := make([]int, len(elems))
	randIDs := make([]int64, len(elems))
	for i, e := range elems {
		ids[i] = e.Msg().ID
		randIDs[i] = f.rand.Int63()
	}

	req := &tg.MessagesForwardMessagesRequest{
		Silent:            first.AsSilent(),
		Background:        false,
		WithMyScore:       false,
		DropAuthor:        first.AsDropAuthor(),
		DropMediaCaptions: false,
		Noforwards:        false,
		FromPeer:          first.From().InputPeer(),
		ID:                ids,
		RandomID:          randIDs,
		ToPeer:            first.To().InputPeer(),
		TopMsgID:          first.Thread(),
		ScheduleDate:      0,
		SendAs:            nil,
	}
	req.SetFlags()

	log := logctx.From(ctx).With(
		zap.Int64("from", first.From().ID()),
		zap.Int64("to", first.To().ID()),
		zap.Int("batch_size", len(ids)))

	if _, err := f.forwardClient(ctx, first).MessagesForwardMessages(ctx, req); err != nil {
		log.Warn("Batch forward failed, falling back to individual",
			zap.Error(err))
		// Fall back to individual forwarding.  forwardMessage
		// handles its own OnAdd/OnDone, so we skip OnDone here.
		var firstErr error
		for _, e := range elems {
			if err := f.forwardMessage(ctx, e); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		return firstErr
	}

	// Success: mark all as done.
	for _, e := range elems {
		f.opts.Progress.OnDone(e, nil)
	}
	return nil
}

func (f *Forwarder) forwardMessage(ctx context.Context, elem Elem, grouped ...*tg.Message) (rerr error) {
	defer func() {
		f.mu.Lock()
		f.sent[f.tuple(elem.From(), elem.Msg())] = struct{}{}

		// grouped message also should be marked as sent
		for _, m := range grouped {
			f.sent[f.tuple(elem.From(), m)] = struct{}{}
		}
		f.mu.Unlock()
		f.opts.Progress.OnDone(elem, rerr)
	}()

	log := logctx.From(ctx).With(
		zap.Int64("from", elem.From().ID()),
		zap.Int64("to", elem.To().ID()),
		zap.Int("message", elem.Msg().ID))

	// used for clone progress
	totalSize, err := mediaSizeSum(elem.Msg(), grouped...)
	if err != nil {
		return errors.Wrap(err, "media total size")
	}
	done := atomic.NewInt64(0)

	forwardTextOnly := func(msg *tg.Message) error {
		if msg.Message == "" {
			return errors.Errorf("empty message content, skip send: %d", msg.ID)
		}
		req := &tg.MessagesSendMessageRequest{
			NoWebpage:              false,
			Silent:                 elem.AsSilent(),
			Background:             false,
			ClearDraft:             false,
			Noforwards:             false,
			UpdateStickersetsOrder: false,
			Peer:                   elem.To().InputPeer(),
			ReplyTo:                getReplyTo(elem.Thread()),
			Message:                msg.Message,
			RandomID:               f.rand.Int63(),
			ReplyMarkup:            msg.ReplyMarkup,
			Entities:               msg.Entities,
			ScheduleDate:           0,
			SendAs:                 nil,
		}
		req.SetFlags()

		if _, err := f.forwardClient(ctx, elem).MessagesSendMessage(ctx, req); err != nil {
			return errors.Wrap(err, "send message")
		}
		return nil
	}

	convForwardedMedia := func(msg *tg.Message) (tg.InputMediaClass, error) {
		if _, hasMedia := msg.GetMedia(); !hasMedia {
			// media can't be forwarded via simple copy(it depends on the server ids)
			// if it's not a media message, just break and send text copy
			return nil, errors.Errorf("message %d is not a media message", msg.ID)
		}

		// if it's a media message, but it's not protected, convert it to InputMediaClass
		// or if it's protected, but it doesn't contain photo or document,

		// we should clone photo and document via re-upload, it will be banned if we forward it directly.
		// but other media can be forwarded directly via copy
		if (!protectedDialog(elem.From()) && !protectedMessage(msg)) || !photoOrDocument(msg.Media) {
			media, ok := tmedia.ConvInputMedia(msg.Media)
			if !ok {
				return nil, errors.Errorf("can't convert message %d to input class directly", msg.ID)
			}
			return media, nil
		}

		media, ok := tmedia.GetMedia(msg)
		if !ok {
			log.Warn("Can't get media from message",
				zap.Int64("peer", elem.From().ID()),
				zap.Int("message", msg.ID))

			// unsupported re-upload media
			return nil, errors.Errorf("unsupported media %T", msg.Media)
		}

		mediaFile, err := f.cloneMedia(ctx, cloneOptions{
			elem:  elem,
			media: media,
			progress: &wrapProgress{
				elem:     elem,
				progress: f.opts.Progress,
				done:     done,
				total:    totalSize * 2,
			},
		}, elem.AsDryRun())
		if err != nil {
			return nil, errors.Wrap(err, "clone media")
		}

		var inputMedia tg.InputMediaClass
		// now we only have to process cloned photo or document
		switch m := msg.Media.(type) {
		case *tg.MessageMediaPhoto:
			photo := &tg.InputMediaUploadedPhoto{
				Spoiler:    m.Spoiler,
				File:       mediaFile,
				TTLSeconds: m.TTLSeconds,
			}
			photo.SetFlags()

			inputMedia = photo
		case *tg.MessageMediaDocument:
			doc, ok := m.Document.AsNotEmpty()
			if !ok {
				return nil, errors.Errorf("empty document %d", msg.ID)
			}

			document := &tg.InputMediaUploadedDocument{
				NosoundVideo: false, // do not set
				ForceFile:    false, // do not set
				Spoiler:      m.Spoiler,
				File:         mediaFile,
				MimeType:     doc.MimeType,
				Attributes:   doc.Attributes,
				Stickers:     nil, // do not set
				TTLSeconds:   0,   // do not set
			}

			if thumb, ok := tmedia.GetDocumentThumb(doc); ok {
				thumbFile, err := f.cloneMedia(ctx, cloneOptions{
					elem:     elem,
					media:    thumb,
					progress: nopProgress{},
				}, elem.AsDryRun())
				if err != nil {
					return nil, errors.Wrap(err, "clone thumb")
				}

				document.Thumb = thumbFile
			}

			document.SetFlags()

			inputMedia = document
		default:
			return nil, errors.Errorf("unsupported media %T", msg.Media)
		}

		// note that they must be separately uploaded using messages uploadMedia first,
		// using raw inputMediaUploaded* constructors is not supported.
		//
		// FIX: MessagesUploadMedia may not preserve the document attributes
		// (NosoundVideo, ForceFile, Attributes) properly, causing media to
		// lose its display type (video → file, etc.). Use the
		// InputMediaUploadedDocument directly in MessagesSendMedia instead.
		// The file has already been uploaded via upload.saveFilePart.
		if elem.AsDryRun() {
			// In dry-run mode, we can't use uploadMedia either — just
			// return a placeholder for counting purposes.
			fallback := &tg.InputMediaDocument{}
			fallback.SetFlags()
			return fallback, nil
		}

		return inputMedia, nil
	}

	switch elem.Mode() {
	case ModeDirect:
		// it can be forwarded via API
		if !protectedDialog(elem.From()) && !protectedMessage(elem.Msg()) {
			directForward := func(ids ...int) error {
				randIDs := make([]int64, 0, len(ids))
				for range ids {
					randIDs = append(randIDs, f.rand.Int63())
				}

				req := &tg.MessagesForwardMessagesRequest{
					Silent:            elem.AsSilent(),
					Background:        false,
					WithMyScore:       false,
					DropAuthor:        elem.AsDropAuthor(),
					DropMediaCaptions: false,
					Noforwards:        false,
					FromPeer:          elem.From().InputPeer(),
					ID:                ids,
					RandomID:          randIDs,
					ToPeer:            elem.To().InputPeer(),
					TopMsgID:          elem.Thread(),
					ScheduleDate:      0,
					SendAs:            nil,
				}
				req.SetFlags()
				if _, err := f.forwardClient(ctx, elem).MessagesForwardMessages(ctx, req); err != nil {
					return errors.Wrap(err, "directly forward")
				}
				return nil
			}

			if len(grouped) > 0 {
				ids := make([]int, 0, len(grouped))
				for _, m := range grouped {
					ids = append(ids, m.ID)
				}

				if err = directForward(ids...); err != nil {
					goto fallback
				}

				return nil
			}

			if err = directForward(elem.Msg().ID); err != nil {
				goto fallback
			}
			return nil
		}
	fallback:
		fallthrough
	case ModeClone:
		if len(grouped) > 0 {
			media := make([]tg.InputSingleMedia, 0, len(grouped))
			for _, gm := range grouped {
				m, err := convForwardedMedia(gm)
				if err != nil {
					log.Debug("Can't convert forwarded media", zap.Error(err))
					continue
				}

				single := tg.InputSingleMedia{
					Media:    m,
					RandomID: f.rand.Int63(),
					Message:  gm.Message,
					Entities: gm.Entities,
				}
				single.SetFlags()

				media = append(media, single)
			}

			if len(media) > 0 {
				req := &tg.MessagesSendMultiMediaRequest{
					Silent:                 elem.AsSilent(),
					Background:             false,
					ClearDraft:             false,
					Noforwards:             false,
					UpdateStickersetsOrder: false,
					Peer:                   elem.To().InputPeer(),
					ReplyTo:                getReplyTo(elem.Thread()),
					MultiMedia:             media,
					ScheduleDate:           0,
					SendAs:                 nil,
				}
				req.SetFlags()
				if _, err := f.forwardClient(ctx, elem).MessagesSendMultiMedia(ctx, req); err != nil {
					return errors.Wrap(err, "send multi media")
				}
				return nil
			}

			return forwardTextOnly(elem.Msg())
		}

		media, err := convForwardedMedia(elem.Msg())
		if err != nil {
			log.Debug("Can't convert forwarded media", zap.Error(err))
			return forwardTextOnly(elem.Msg())
		}
		// send text copy with forwarded media
		req := &tg.MessagesSendMediaRequest{
			Silent:                 elem.AsSilent(),
			Background:             false,
			ClearDraft:             false,
			Noforwards:             false,
			UpdateStickersetsOrder: false,
			Peer:                   elem.To().InputPeer(),
			ReplyTo:                getReplyTo(elem.Thread()),
			Media:                  media,
			Message:                elem.Msg().Message,
			RandomID:               f.rand.Int63(),
			ReplyMarkup:            elem.Msg().ReplyMarkup,
			Entities:               elem.Msg().Entities,
			ScheduleDate:           0,
			SendAs:                 nil,
		}
		req.SetFlags()

		if _, err := f.forwardClient(ctx, elem).MessagesSendMedia(ctx, req); err != nil {
			return errors.Wrap(err, "send single media")
		}
		return nil
	}

	return errors.Errorf("unsupported mode %v", elem.Mode())
}

func (f *Forwarder) tuple(peer peers.Peer, msg *tg.Message) tuple {
	return tuple{
		from: peer.ID(),
		msg:  msg.ID,
	}
}

type nopInvoker struct{}

func (n nopInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	return nil
}

type nopProgress struct{}

func (nopProgress) add(_ int64) {}

type wrapProgress struct {
	elem     Elem
	progress ProgressClone
	done     *atomic.Int64
	total    int64
}

func (w *wrapProgress) add(n int64) {
	w.progress.OnClone(w.elem, ProgressState{
		Done:  w.done.Add(n),
		Total: w.total,
	})
}

func (f *Forwarder) forwardClient(ctx context.Context, elem Elem) *tg.Client {
	if elem.AsDryRun() {
		return tg.NewClient(nopInvoker{})
	}

	return f.opts.Pool.Default(ctx)
}

func protectedDialog(peer peers.Peer) bool {
	switch p := peer.(type) {
	case peers.Chat:
		return p.Raw().GetNoforwards()
	case peers.Channel:
		return p.Raw().GetNoforwards()
	}

	return false
}

func protectedMessage(msg *tg.Message) bool {
	return msg.GetNoforwards()
}

func photoOrDocument(media tg.MessageMediaClass) bool {
	switch media.(type) {
	case *tg.MessageMediaPhoto, *tg.MessageMediaDocument:
		return true
	default:
		return false
	}
}

func mediaSizeSum(msg *tg.Message, grouped ...*tg.Message) (int64, error) {
	if len(grouped) > 0 {
		total := int64(0)
		for _, gm := range grouped {
			m, ok := tmedia.GetMedia(gm)
			if !ok {
				return 0, errors.Errorf("can't get media from message %d", gm.ID)
			}
			total += m.Size
		}

		return total, nil
	}

	m, ok := tmedia.GetMedia(msg)
	if !ok { // maybe it's a text only message
		return 0, nil
	}

	return m.Size, nil
}

func getReplyTo(thread int) tg.InputReplyToClass {
	replyTo := &tg.InputReplyToMessage{
		ReplyToMsgID: thread,
	}
	replyTo.SetFlags()

	return replyTo
}
