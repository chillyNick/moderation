package moderation

import (
	"github.com/homework3/moderation/pkg/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"math/rand"
)

var moderationFailed = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "moderation_comment_failed",
		Help: "A number of comments which failed to pass moderation",
	},
)

var moderationSuccess = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "moderation_comment_success",
		Help: "A number of comments which passed moderation",
	},
)

func ModerateComment(comment model.Comment) *model.ModerationComment {
	mComment := &model.ModerationComment{
		CommentId: comment.Id,
		UserId:    comment.UserId,
		ItemId:    comment.ItemId,
	}

	if rand.Intn(10) < 5 {
		mComment.Status = model.ModerationCommentStatusFailed
		moderationFailed.Inc()
	} else {
		mComment.Status = model.ModerationCommentStatusPassed
		moderationSuccess.Inc()
	}
	mComment.Reason = "By random reason"

	return mComment
}
