package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/expr-lang/expr"
	"github.com/fatih/color"
	"github.com/go-faster/jx"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
	"github.com/jedib0t/go-pretty/v6/progress"
	"go.uber.org/multierr"

	"github.com/fbicloud/tdl/core/storage"
	"github.com/fbicloud/tdl/core/tmedia"
	"github.com/fbicloud/tdl/core/util/tutil"
	"github.com/fbicloud/tdl/pkg/prog"
	"github.com/fbicloud/tdl/pkg/texpr"
)

//go:generate go-enum --names --values --flag --nocase

type ExportOptions struct {
	Type        ExportType
	Chat        string
	Thread      int // topic id in forum, message id in group
	Input       []int
	Output      string
	Filter      string
	From        string // user id or username to filter by sender
	OnlyMedia   bool
	WithContent bool
	Raw         bool
	All         bool
	Dedup       bool // skip duplicate media within the same chat
}

type Message struct {
	ID   int         `json:"id"`
	Type string      `json:"type"`
	File string      `json:"file"`
	Date int         `json:"date,omitempty"`
	Text string      `json:"text,omitempty"`
	Raw  *tg.Message `json:"raw,omitempty"`
}

// ExportType
// ENUM(time, id, last)
type ExportType int

func Export(ctx context.Context, c *telegram.Client, kvd storage.Storage, opts ExportOptions) (rerr error) {
	// only output available fields
	if opts.Filter == "-" {
		fg := texpr.NewFieldsGetter(nil)

		fields, err := fg.Walk(&texpr.EnvMessage{})
		if err != nil {
			return fmt.Errorf("failed to walk fields: %w", err)
		}

		fmt.Print(fg.Sprint(fields, true))
		return nil
	}

	filter, err := expr.Compile(opts.Filter, expr.AsBool())
	if err != nil {
		return fmt.Errorf("failed to compile filter: %w", err)
	}

	var peer peers.Peer

	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(c.API())
	if opts.Chat == "" { // defaults to me(saved messages)
		peer, err = manager.Self(ctx)
	} else {
		peer, err = tutil.GetInputPeer(ctx, manager, opts.Chat)
	}
	if err != nil {
		return fmt.Errorf("failed to get peer: %w", err)
	}

	color.Yellow("WARN: Export only generates minimal JSON for tdl download, not for backup.")
	color.Cyan("Occasional suspensions are due to Telegram rate limitations, please wait a moment.")
	fmt.Println()

	color.Blue("Type: %s | Input: %v", opts.Type, opts.Input)

	pw := prog.New(progress.FormatNumber)
	pw.SetUpdateFrequency(200 * time.Millisecond)
	pw.Style().Visibility.TrackerOverall = false
	pw.Style().Visibility.ETA = false
	pw.Style().Visibility.Percentage = false

	tracker := prog.AppendTracker(pw, progress.FormatNumber, fmt.Sprintf("%s-%d", peer.VisibleName(), peer.ID()), 0)

	go pw.Render()

	var q messages.Query
	switch {
	case opts.From != "": // filter by sender using messages.search API
		fromPeer, err := tutil.GetInputPeer(ctx, manager, opts.From)
		if err != nil {
			return fmt.Errorf("failed to resolve --from user: %w", err)
		}
		search := query.NewQuery(c.API()).Messages().
			Search(peer.InputPeer()).
			FromID(fromPeer.InputPeer()).
			BatchSize(100)
		if opts.Thread != 0 {
			search = search.TopMsgID(opts.Thread)
		}
		q = search
	case opts.Thread != 0: // topic messages, reply messages
		q = query.NewQuery(c.API()).Messages().GetReplies(peer.InputPeer()).MsgID(opts.Thread)
	default: // history
		q = query.NewQuery(c.API()).Messages().GetHistory(peer.InputPeer())
	}

	// Build the iterator. When exporting media-only from history
	// (no --from, no --thread), use server-side media filters via
	// messages.Search to avoid fetching non-media messages.
	var iter iterator
	if !opts.All && opts.From == "" && opts.Thread == 0 {
		searchA := query.NewQuery(c.API()).Messages().
			Search(peer.InputPeer()).
			Filter(&tg.InputMessagesFilterPhotoVideo{}).
			BatchSize(100)
		searchB := query.NewQuery(c.API()).Messages().
			Search(peer.InputPeer()).
			Filter(&tg.InputMessagesFilterDocument{}).
			BatchSize(100)
		itA := messages.NewIterator(searchA, 100)
		itB := messages.NewIterator(searchB, 100)
		switch opts.Type {
		case ExportTypeTime:
			itA = itA.OffsetDate(opts.Input[1] + 1)
			itB = itB.OffsetDate(opts.Input[1] + 1)
		case ExportTypeId:
			itA = itA.OffsetID(opts.Input[1] + 1)
			itB = itB.OffsetID(opts.Input[1] + 1)
		case ExportTypeLast:
		}
		iter = &mediaIter{a: itA, b: itB}
	} else {
		it := messages.NewIterator(q, 100)
		switch opts.Type {
		case ExportTypeTime:
			it = it.OffsetDate(opts.Input[1] + 1)
		case ExportTypeId:
			it = it.OffsetID(opts.Input[1] + 1) // #89: retain the last msg id
		case ExportTypeLast:
		}
		iter = &singleIter{it}
	}

	f, err := os.Create(opts.Output)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(f))

	enc := jx.NewStreamingEncoder(f, 512)
	defer multierr.AppendInvoke(&rerr, multierr.Close(enc))

	// process thread is reply type and peer is broadcast channel,
	// so we need to set discussion group id instead of broadcast id
	id := peer.ID()
	if p, ok := peer.(peers.Channel); opts.Thread != 0 && ok && p.IsBroadcast() {
		bc, _ := p.ToBroadcast()
		raw, err := bc.FullRaw(ctx)
		if err != nil {
			return fmt.Errorf("failed to get broadcast full raw: %w", err)
		}

		if id, ok = raw.GetLinkedChatID(); !ok {
			return fmt.Errorf("no linked group")
		}
	}

	enc.ObjStart()
	defer enc.ObjEnd()
	enc.Field("id", func(e *jx.Encoder) { e.Int64(id) })

	enc.FieldStart("messages")
	enc.ArrStart()
	defer enc.ArrEnd()

	count := int64(0)
	seenMedia := make(map[string]struct{})

loop:
	for iter.Next(ctx) {
		msg := iter.Value()
		switch opts.Type {
		case ExportTypeTime:
			if msg.Msg.GetDate() < opts.Input[0] {
				break loop
			}
		case ExportTypeId:
			if msg.Msg.GetID() < opts.Input[0] {
				break loop
			}
		case ExportTypeLast:
			if count >= int64(opts.Input[0]) {
				break loop
			}
		}

		m, ok := msg.Msg.(*tg.Message)
		if !ok {
			continue
		}
		// only get media messages
		media, ok := tmedia.GetMedia(m)
		if !ok && !opts.All {
			continue
		}

		// dedup: skip duplicate media within the same chat.
		// Same file shared multiple times (e.g. re-forwarded) has the
		// same server-side media ID and is treated as duplicate.
		if opts.Dedup && media != nil {
			if key := media.GetMediaUniqueKey(); key != "" {
				key = strconv.FormatInt(id, 10) + "/" + key
				if _, seen := seenMedia[key]; seen {
					continue
				}
				seenMedia[key] = struct{}{}
			}
		}

		b, err := texpr.Run(filter, texpr.ConvertEnvMessage(m))
		if err != nil {
			return fmt.Errorf("failed to run filter: %w", err)
		}
		if !b.(bool) { // filtered
			continue
		}

		fileName := ""
		if media != nil { // #207
			fileName = media.Name
		}
		t := &Message{
			ID:   m.ID,
			Type: "message",
			File: fileName,
		}
		if opts.WithContent {
			t.Date = m.Date
			t.Text = m.Message
		}
		if opts.Raw {
			t.Raw = m
		}

		mb, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		enc.Raw(mb)

		count++
		tracker.SetValue(count)
	}

	if err = iter.Err(); err != nil {
		return err
	}

	tracker.MarkAsDone()
	prog.Wait(ctx, pw)
	return nil
}

// iterator abstracts a message source so the export loop works with both
// a single messages.Iterator and a merged pair for media-only searches.
type iterator interface {
	Next(ctx context.Context) bool
	Value() messages.Elem
	Err() error
}

// singleIter wraps a single messages.Iterator for the standard code path.
type singleIter struct {
	it *messages.Iterator
}

func (s *singleIter) Next(ctx context.Context) bool { return s.it.Next(ctx) }
func (s *singleIter) Value() messages.Elem           { return s.it.Value() }
func (s *singleIter) Err() error                     { return s.it.Err() }

// mediaIter merges two messages.Iterator instances (photo+video and
// document search results) into a single ID-descending stream.
// Both iterators must produce messages in descending ID order.
type mediaIter struct {
	a, b  *messages.Iterator
	nextA messages.Elem
	nextB messages.Elem
	hasA  bool
	hasB  bool
	errA  error
	errB  error
	cur   messages.Elem
}

func (m *mediaIter) Next(ctx context.Context) bool {
	// refill buffers
	if !m.hasA && m.errA == nil {
		if m.a.Next(ctx) {
			m.nextA = m.a.Value()
			m.hasA = true
		} else {
			m.errA = m.a.Err()
		}
	}
	if !m.hasB && m.errB == nil {
		if m.b.Next(ctx) {
			m.nextB = m.b.Value()
			m.hasB = true
		} else {
			m.errB = m.b.Err()
		}
	}
	if !m.hasA && !m.hasB {
		return false
	}
	// pick the message with the higher ID to maintain descending order
	if m.hasA && (!m.hasB || m.nextA.Msg.GetID() >= m.nextB.Msg.GetID()) {
		m.cur = m.nextA
		m.hasA = false
		return true
	}
	m.cur = m.nextB
	m.hasB = false
	return true
}

func (m *mediaIter) Value() messages.Elem { return m.cur }

func (m *mediaIter) Err() error {
	if m.errA != nil {
		return m.errA
	}
	return m.errB
}
