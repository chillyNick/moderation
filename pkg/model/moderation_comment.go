package model

const (
	ModerationCommentStatusFailed = "failed"
	ModerationCommentStatusPassed = "passed"
)

type ModerationComment struct {
	CommentId int64
	UserId    int32
	ItemId    int32
	Status    string
	Reason    string
}
