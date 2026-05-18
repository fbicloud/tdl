package tmedia

import (
	"strconv"

	"github.com/gotd/td/tg"
)

type Media struct {
	InputFileLoc tg.InputFileLocationClass // mtproto file location of the media file
	Name         string                    // file name
	Size         int64                     // size in bytes
	DC           int                       // which DC the media is stored
	Date         int64                     // media creation(upload) timestamp
}

func (m *Media) GetMediaID() int64 {
	switch loc := m.InputFileLoc.(type) {
	case *tg.InputPhotoFileLocation:
		return loc.GetID()
	case *tg.InputDocumentFileLocation:
		return loc.GetID()
	default:
		return 0
	}
}

func (m *Media) GetMediaUniqueKey() string {
	switch loc := m.InputFileLoc.(type) {
	case *tg.InputPhotoFileLocation:
		return "photo/" + strconv.FormatInt(loc.GetID(), 10)
	case *tg.InputDocumentFileLocation:
		return "doc/" + strconv.FormatInt(loc.GetID(), 10)
	default:
		return ""
	}
}

func ExtractMedia(m tg.MessageMediaClass) (*Media, bool) {
	switch m := m.(type) {
	case *tg.MessageMediaPhoto:
		return GetPhotoInfo(m)
	case *tg.MessageMediaDocument:
		return GetDocumentInfo(m)
	case *tg.MessageMediaInvoice:
		return GetExtendedMedia(m.ExtendedMedia)
	}
	return nil, false
}

func GetMedia(msg tg.MessageClass) (*Media, bool) {
	mm, ok := msg.(*tg.Message)
	if !ok {
		return nil, false
	}

	media, ok := mm.GetMedia()
	if !ok {
		return nil, false
	}

	return ExtractMedia(media)
}

func GetExtendedMedia(mm tg.MessageExtendedMediaClass) (*Media, bool) {
	m, ok := mm.(*tg.MessageExtendedMedia)
	if !ok {
		return nil, false
	}
	return ExtractMedia(m.Media)
}

func GetDocumentThumb(doc *tg.Document) (*Media, bool) {
	thumbs, exists := doc.GetThumbs()
	if !exists {
		return nil, false
	}

	photoSize := &tg.PhotoSize{}
	for _, t := range thumbs {
		if p, ok := t.(*tg.PhotoSize); ok {
			photoSize = p
			break
		}
	}

	if photoSize == nil {
		return nil, false
	}

	return &Media{
		InputFileLoc: &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
			ThumbSize:     photoSize.Type,
		},
		Name: "thumb.jpg",
		Size: int64(photoSize.Size),
		DC:   doc.DCID,
		Date: int64(doc.Date),
	}, true
}
