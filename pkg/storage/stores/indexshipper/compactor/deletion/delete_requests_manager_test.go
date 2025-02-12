package deletion

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/storage/stores/indexshipper/compactor/deletionmode"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/pkg/logql/syntax"
	"github.com/grafana/loki/pkg/storage/stores/indexshipper/compactor/retention"
)

const testUserID = "test-user"

func TestDeleteRequestsManager_Expired(t *testing.T) {
	type resp struct {
		isExpired           bool
		nonDeletedIntervals []retention.IntervalFilter
	}

	now := model.Now()
	lblFoo, err := syntax.ParseLabels(`{foo="bar"}`)
	require.NoError(t, err)

	chunkEntry := retention.ChunkEntry{
		ChunkRef: retention.ChunkRef{
			UserID:  []byte(testUserID),
			From:    now.Add(-12 * time.Hour),
			Through: now.Add(-time.Hour),
		},
		Labels: lblFoo,
	}

	for _, tc := range []struct {
		name                        string
		deletionMode                deletionmode.Mode
		deleteRequestsFromStore     []DeleteRequest
		batchSize                   int
		expectedResp                resp
		expectedDeletionRangeByUser map[string]model.Interval
	}{
		{
			name:         "no delete requests",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
		},
		{
			name:         "no relevant delete requests",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    "different-user",
					Query:     lblFoo.String(),
					StartTime: now.Add(-24 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				"different-user": {
					Start: now.Add(-24 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "whole chunk deleted by single request",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-24 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           true,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-24 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "deleted interval out of range",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-48 * time.Hour),
					EndTime:   now.Add(-24 * time.Hour),
				},
			},
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-48 * time.Hour),
					End:   now.Add(-24 * time.Hour),
				},
			},
		},
		{
			name:         "deleted interval out of range(with multiple user requests)",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-48 * time.Hour),
					EndTime:   now.Add(-24 * time.Hour),
				},
				{
					UserID:    "different-user",
					Query:     lblFoo.String(),
					StartTime: now.Add(-24 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-48 * time.Hour),
					End:   now.Add(-24 * time.Hour),
				},
				"different-user": {
					Start: now.Add(-24 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "multiple delete requests with one deleting the whole chunk",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-48 * time.Hour),
					EndTime:   now.Add(-24 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-12 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           true,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-48 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "multiple delete requests causing multiple holes",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-13 * time.Hour),
					EndTime:   now.Add(-11 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-10 * time.Hour),
					EndTime:   now.Add(-8 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-6 * time.Hour),
					EndTime:   now.Add(-5 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired: true,
				nonDeletedIntervals: []retention.IntervalFilter{
					{
						Interval: model.Interval{
							Start: now.Add(-11*time.Hour) + 1,
							End:   now.Add(-10*time.Hour) - 1,
						},
					},
					{
						Interval: model.Interval{
							Start: now.Add(-8*time.Hour) + 1,
							End:   now.Add(-6*time.Hour) - 1,
						},
					},
					{
						Interval: model.Interval{
							Start: now.Add(-5*time.Hour) + 1,
							End:   now.Add(-2*time.Hour) - 1,
						},
					},
				},
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-13 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "multiple overlapping requests deleting the whole chunk",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-13 * time.Hour),
					EndTime:   now.Add(-6 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-8 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           true,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-13 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "multiple non-overlapping requests deleting the whole chunk",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-12 * time.Hour),
					EndTime:   now.Add(-6*time.Hour) - 1,
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-6 * time.Hour),
					EndTime:   now.Add(-4*time.Hour) - 1,
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-4 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           true,
				nonDeletedIntervals: nil,
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-12 * time.Hour),
					End:   now,
				},
			},
		},
		{
			name:         "deletes are disabled",
			deletionMode: deletionmode.Disabled,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-13 * time.Hour),
					EndTime:   now.Add(-11 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-10 * time.Hour),
					EndTime:   now.Add(-8 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-6 * time.Hour),
					EndTime:   now.Add(-5 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
		},
		{
			name:         "deletes are `filter-only`",
			deletionMode: deletionmode.FilterOnly,
			batchSize:    70,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-13 * time.Hour),
					EndTime:   now.Add(-11 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-10 * time.Hour),
					EndTime:   now.Add(-8 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-6 * time.Hour),
					EndTime:   now.Add(-5 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired:           false,
				nonDeletedIntervals: nil,
			},
		},
		{
			name:         "Deletes are limited by batch size",
			deletionMode: deletionmode.FilterAndDelete,
			batchSize:    2,
			deleteRequestsFromStore: []DeleteRequest{
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-13 * time.Hour),
					EndTime:   now.Add(-11 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-10 * time.Hour),
					EndTime:   now.Add(-8 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-6 * time.Hour),
					EndTime:   now.Add(-5 * time.Hour),
				},
				{
					UserID:    testUserID,
					Query:     lblFoo.String(),
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now,
				},
			},
			expectedResp: resp{
				isExpired: true,
				nonDeletedIntervals: []retention.IntervalFilter{
					{
						Interval: model.Interval{
							Start: now.Add(-11*time.Hour) + 1,
							End:   now.Add(-10*time.Hour) - 1,
						},
					},
					{
						Interval: model.Interval{
							Start: now.Add(-8*time.Hour) + 1,
							End:   now.Add(-time.Hour),
						},
					},
				},
			},
			expectedDeletionRangeByUser: map[string]model.Interval{
				testUserID: {
					Start: now.Add(-13 * time.Hour),
					End:   now.Add(-8 * time.Hour),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := NewDeleteRequestsManager(&mockDeleteRequestsStore{deleteRequests: tc.deleteRequestsFromStore}, time.Hour, tc.batchSize, &fakeLimits{mode: tc.deletionMode.String()}, nil)
			require.NoError(t, mgr.loadDeleteRequestsToProcess())

			for _, deleteRequests := range mgr.deleteRequestsToProcess {
				for _, dr := range deleteRequests.requests {
					require.EqualValues(t, 0, dr.DeletedLines)
				}
			}

			isExpired, nonDeletedIntervals := mgr.Expired(chunkEntry, model.Now())
			require.Equal(t, tc.expectedResp.isExpired, isExpired)
			for idx, interval := range nonDeletedIntervals {
				require.Equal(t, tc.expectedResp.nonDeletedIntervals[idx].Interval.Start, interval.Interval.Start)
				require.Equal(t, tc.expectedResp.nonDeletedIntervals[idx].Interval.End, interval.Interval.End)
				require.NotNil(t, interval.Filter)
			}

			require.Equal(t, len(tc.expectedDeletionRangeByUser), len(mgr.deleteRequestsToProcess))
			for userID, dr := range tc.expectedDeletionRangeByUser {
				require.Equal(t, dr, mgr.deleteRequestsToProcess[userID].requestsInterval)
			}
		})
	}
}

func TestDeleteRequestsManager_IntervalMayHaveExpiredChunks(t *testing.T) {
	tt := []struct {
		deleteRequestsFromStore []DeleteRequest
		hasChunks               bool
		user                    string
	}{
		{[]DeleteRequest{{Query: `0`, UserID: "test-user", StartTime: 0, EndTime: 100}}, true, "test-user"},
		{[]DeleteRequest{{Query: `1`, UserID: "test-user", StartTime: 200, EndTime: 400}}, true, "test-user"},
		{[]DeleteRequest{{Query: `2`, UserID: "test-user", StartTime: 400, EndTime: 500}}, true, "test-user"},
		{[]DeleteRequest{{Query: `3`, UserID: "test-user", StartTime: 500, EndTime: 700}}, true, "test-user"},
		{[]DeleteRequest{{Query: `3`, UserID: "other-user", StartTime: 500, EndTime: 700}}, false, "test-user"},
		{[]DeleteRequest{{Query: `4`, UserID: "test-user", StartTime: 700, EndTime: 900}}, true, "test-user"},
		{[]DeleteRequest{{Query: `4`, UserID: "", StartTime: 700, EndTime: 900}}, true, ""},
		{[]DeleteRequest{}, false, ""},
	}

	for _, tc := range tt {
		mgr := NewDeleteRequestsManager(&mockDeleteRequestsStore{deleteRequests: tc.deleteRequestsFromStore}, time.Hour, 70, &fakeLimits{mode: deletionmode.FilterAndDelete.String()}, nil)
		require.NoError(t, mgr.loadDeleteRequestsToProcess())

		interval := model.Interval{Start: 300, End: 600}
		require.Equal(t, tc.hasChunks, mgr.IntervalMayHaveExpiredChunks(interval, tc.user))
	}
}

type mockDeleteRequestsStore struct {
	DeleteRequestsStore
	deleteRequests []DeleteRequest
	addedUser      string
	addedStartTime model.Time
	addedEndTime   model.Time
	addedQuery     string
	addErr         error
}

func (m *mockDeleteRequestsStore) GetDeleteRequestsByStatus(_ context.Context, _ DeleteRequestStatus) ([]DeleteRequest, error) {
	return m.deleteRequests, nil
}

func (m *mockDeleteRequestsStore) AddDeleteRequest(ctx context.Context, userID string, startTime, endTime model.Time, query string) error {
	m.addedUser = userID
	m.addedStartTime = startTime
	m.addedEndTime = endTime
	m.addedQuery = query
	return m.addErr
}
