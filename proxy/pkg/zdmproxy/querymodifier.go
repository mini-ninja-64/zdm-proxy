package zdmproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
)

type QueryModifier struct {
	conf              *config.Config
	timeUuidGenerator TimeUuidGenerator
}

func NewQueryModifier(conf *config.Config, timeUuidGenerator TimeUuidGenerator) *QueryModifier {
	return &QueryModifier{conf: conf, timeUuidGenerator: timeUuidGenerator}
}

type LazyFrameCache struct {
	original *frame.Frame
	copy     *frame.Frame
}

func NewLazyFrameCache(original *frame.Frame) *LazyFrameCache {
	return &LazyFrameCache{
		original: original,
	}
}
func (cache *LazyFrameCache) IsCopied() bool {
	return cache.copy != nil
}
func (cache *LazyFrameCache) EnsureCopied() {
	if cache.copy != nil {
		return
	}
	cache.copy = cache.original.DeepCopy()
}

func (cache *LazyFrameCache) Get() *frame.Frame {
	if cache.copy != nil {
		return cache.copy
	}
	return cache.original
}

func (cache *LazyFrameCache) EnsureCopiedAndGet() *frame.Frame {
	cache.EnsureCopied()
	return cache.Get()
}

// TODO: hmmmm this feels gut feeling their might be a bug in here
func (recv *QueryModifier) replaceQueryInBatchMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData,
	clusterType common.ClusterType) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {

	if len(statementsQueryData) == 0 {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}

	newStatementsQueryData := make([]*statementQueryData, 0, len(statementsQueryData))
	statementsReplacedTerms := make([]*statementReplacedTerms, 0)
	replacedStatementIndexes := make([]int, 0)

	for idx, stmtQueryData := range statementsQueryData {
		if recv.conf.ReplaceCqlFunctions && stmtQueryData.queryData.hasNowFunctionCalls() {
			newQueryData, replacedTerms := stmtQueryData.queryData.replaceNowFunctionCallsWithLiteral()
			newStatementsQueryData = append(
				newStatementsQueryData,
				&statementQueryData{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData})
			statementsReplacedTerms = append(
				statementsReplacedTerms,
				&statementReplacedTerms{statementIndex: stmtQueryData.statementIndex, replacedTerms: replacedTerms})
			replacedStatementIndexes = append(replacedStatementIndexes, idx)
		} else if clusterType == common.ClusterTypeTarget && len(recv.conf.KeyspaceMappings) != 0 {
			queryKeyspace := stmtQueryData.queryData.getApplicableKeyspace()
			newKeyspace, keyspaceShouldBeReplaced := recv.conf.KeyspaceMappings[queryKeyspace]
			if keyspaceShouldBeReplaced {
				log.Infof("Replacing keyspace %s with %s", queryKeyspace, newKeyspace)
				newQueryData := stmtQueryData.queryData.replaceKeyspaceName(newKeyspace)
				newStatementsQueryData = append(
					newStatementsQueryData,
					&statementQueryData{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData})
				// TODO: Figure out replaced terms idk
				statementsReplacedTerms = append(
					statementsReplacedTerms,
					&statementReplacedTerms{statementIndex: stmtQueryData.statementIndex, replacedTerms: nil})
				replacedStatementIndexes = append(replacedStatementIndexes, idx)
			}
			newStatementsQueryData = append(newStatementsQueryData, stmtQueryData)
		} else {
			newStatementsQueryData = append(newStatementsQueryData, stmtQueryData)
		}
	}

	if len(replacedStatementIndexes) == 0 {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}

	newFrame := decodedFrame.DeepCopy()
	newBatchMsg, ok := newFrame.Body.Message.(*message.Batch)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Batch in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	for _, idx := range replacedStatementIndexes {
		newStmtQueryData := newStatementsQueryData[idx]
		if newStmtQueryData.statementIndex >= len(newBatchMsg.Children) {
			return nil, nil, nil, fmt.Errorf("new query data statement index (%v) is greater or equal than "+
				"number of batch child statements (%v)", newStmtQueryData.statementIndex, len(newBatchMsg.Children))
		}
		newBatchMsg.Children[newStmtQueryData.statementIndex].Query = newStmtQueryData.queryData.getQuery()
	}

	return newFrame, statementsReplacedTerms, newStatementsQueryData, nil
}

func (recv *QueryModifier) replaceQueryInQueryMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData,
	clusterType common.ClusterType) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {
	frameCache := NewLazyFrameCache(decodedFrame)

	stmtQueryData, err := singleQuery(statementsQueryData)
	if err != nil {
		return nil, nil, nil, err
	}

	_, correctType := frameCache.Get().Body.Message.(*message.Query)
	if !correctType {
		return nil, nil, nil, fmt.Errorf("expected Prepare statement but got %v instead", frameCache.Get().Body.Message.GetOpCode())
	}

	newQueryData := stmtQueryData.queryData
	var replacedTerms []*term
	if recv.conf.ReplaceCqlFunctions && requiresQueryReplacement(stmtQueryData) {
		newQueryData, replacedTerms = stmtQueryData.queryData.replaceNowFunctionCallsWithLiteral()
		frameCache.EnsureCopied()
	}

	if clusterType == common.ClusterTypeTarget && len(recv.conf.KeyspaceMappings) != 0 {
		queryKeyspace := stmtQueryData.queryData.getApplicableKeyspace()
		newKeyspace, keyspaceShouldBeReplaced := recv.conf.KeyspaceMappings[queryKeyspace]
		if keyspaceShouldBeReplaced {
			log.Infof("Replacing keyspace %s with %s", queryKeyspace, newKeyspace)
			newQueryData = stmtQueryData.queryData.replaceKeyspaceName(newKeyspace)
			queryMessage := frameCache.Get().Body.Message.(*message.Query)
			if queryMessage.Options.Keyspace == queryKeyspace {
				queryMessage.Options.Keyspace = newKeyspace
			}
		}
	}

	if !frameCache.IsCopied() {
		return frameCache.Get(), []*statementReplacedTerms{}, statementsQueryData, nil
	}
	frameCache.Get().Body.Message.(*message.Query).Query = newQueryData.getQuery()
	return frameCache.Get(), []*statementReplacedTerms{{0, replacedTerms}}, []*statementQueryData{{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData}}, nil
}

func (recv *QueryModifier) replaceQueryInPrepareMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData,
	clusterType common.ClusterType) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {
	frameCache := NewLazyFrameCache(decodedFrame)

	stmtQueryData, err := singleQuery(statementsQueryData)
	if err != nil {
		return nil, nil, nil, err
	}

	_, correctType := frameCache.Get().Body.Message.(*message.Prepare)
	if !correctType {
		return nil, nil, nil, fmt.Errorf("expected Prepare statement but got %v instead", frameCache.Get().Body.Message.GetOpCode())
	}

	newQueryData := stmtQueryData.queryData
	var replacedTerms []*term
	if recv.conf.ReplaceCqlFunctions && requiresQueryReplacement(stmtQueryData) {
		if stmtQueryData.queryData.hasNamedBindMarkers() {
			newQueryData, replacedTerms = stmtQueryData.queryData.replaceNowFunctionCallsWithNamedBindMarkers()
		} else {
			newQueryData, replacedTerms = stmtQueryData.queryData.replaceNowFunctionCallsWithPositionalBindMarkers()
		}
		frameCache.EnsureCopied()
	}

	if clusterType == common.ClusterTypeTarget && len(recv.conf.KeyspaceMappings) != 0 {
		queryKeyspace := stmtQueryData.queryData.getApplicableKeyspace()
		newKeyspace, keyspaceShouldBeReplaced := recv.conf.KeyspaceMappings[queryKeyspace]
		if keyspaceShouldBeReplaced {
			log.Infof("Replacing keyspace %s with %s", queryKeyspace, newKeyspace)
			newQueryData = stmtQueryData.queryData.replaceKeyspaceName(newKeyspace)
			queryMessage := frameCache.Get().Body.Message.(*message.Prepare)
			if queryMessage.Keyspace == queryKeyspace {
				queryMessage.Keyspace = newKeyspace
			}
		}
	}
	if !frameCache.IsCopied() {
		return frameCache.Get(), []*statementReplacedTerms{}, statementsQueryData, nil
	}
	frameCache.Get().Body.Message.(*message.Prepare).Query = newQueryData.getQuery()
	return frameCache.Get(), []*statementReplacedTerms{{0, replacedTerms}}, []*statementQueryData{{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData}}, nil
}

func requiresQueryReplacement(stmtQueryData *statementQueryData) bool {
	return stmtQueryData.queryData.hasNowFunctionCalls()
}

func singleQuery(statementsQueryData []*statementQueryData) (*statementQueryData, error) {
	if len(statementsQueryData) != 1 {
		return nil, fmt.Errorf("expected single query data object but got %v", len(statementsQueryData))
	}
	return statementsQueryData[0], nil
}

type InFlightFrame struct {
	decodeContext *frameDecodeContext
	replacedTerms []*statementReplacedTerms
}

func (recv *QueryModifier) processAndBifurcate(currentKeyspace string, context *frameDecodeContext) (*InFlightFrame, *InFlightFrame, error) {
	// Note optimisation for no replacement users
	if !recv.conf.ReplaceCqlFunctions && len(recv.conf.KeyspaceMappings) == 0 {
		return &InFlightFrame{decodeContext: context}, &InFlightFrame{decodeContext: context}, nil
	}

	originFrame, statementsQueryData, err := context.GetOrDecodeAndInspect(currentKeyspace, recv.timeUuidGenerator)
	if err != nil {
		if errors.Is(err, NotInspectableErr) {
			return &InFlightFrame{decodeContext: context}, &InFlightFrame{decodeContext: context}, nil
		}
		return nil, nil, fmt.Errorf("could not check whether query needs replacement for a '%v' request: %w",
			context.GetRawFrame().Header.OpCode.String(), err)
	}

	targetFrame := originFrame.DeepCopy()
	requestType := context.GetRawFrame().Header.OpCode

	var originNewFrame *frame.Frame
	var originReplacedTerms []*statementReplacedTerms
	var originNewStatementsQueryData []*statementQueryData
	var targetNewFrame *frame.Frame
	var targetReplacedTerms []*statementReplacedTerms
	var targetNewStatementsQueryData []*statementQueryData
	var originErr error
	var targetErr error

	// Note: ultimate performance would be gained by bifurcating inside the replace functions,
	//		 however that would make things a lil more complex code wise, so I have opted for
	//       this design where each individual unit of work is completed separately to better
	//       model the problem domain
	switch requestType {
	case primitive.OpCodeBatch:
		originNewFrame, originReplacedTerms, originNewStatementsQueryData, originErr = recv.replaceQueryInBatchMessage(originFrame, statementsQueryData, common.ClusterTypeOrigin)
		targetNewFrame, targetReplacedTerms, targetNewStatementsQueryData, targetErr = recv.replaceQueryInBatchMessage(targetFrame, statementsQueryData, common.ClusterTypeTarget)
	case primitive.OpCodeQuery:
		originNewFrame, originReplacedTerms, originNewStatementsQueryData, originErr = recv.replaceQueryInQueryMessage(originFrame, statementsQueryData, common.ClusterTypeOrigin)
		targetNewFrame, targetReplacedTerms, targetNewStatementsQueryData, targetErr = recv.replaceQueryInQueryMessage(targetFrame, statementsQueryData, common.ClusterTypeTarget)
	case primitive.OpCodePrepare:
		originNewFrame, originReplacedTerms, originNewStatementsQueryData, originErr = recv.replaceQueryInPrepareMessage(originFrame, statementsQueryData, common.ClusterTypeOrigin)
		targetNewFrame, targetReplacedTerms, targetNewStatementsQueryData, targetErr = recv.replaceQueryInPrepareMessage(targetFrame, statementsQueryData, common.ClusterTypeTarget)
	default:
		log.Warnf("Request requires query replacement but op code (%v) unrecognized, this is most likely a bug", requestType.String())
	}

	if originErr != nil {
		return nil, nil, fmt.Errorf("could not replace query string in origin request '%v': %w", requestType.String(), originErr)
	}
	if targetErr != nil {
		return nil, nil, fmt.Errorf("could not replace query string in target request '%v': %w", requestType.String(), targetErr)
	}

	originRawFrame, err := defaultCodec.ConvertToRawFrame(originNewFrame)
	if err != nil {
		return nil, nil, fmt.Errorf("could not convert modified origin frame to raw frame: %w", err)
	}
	targetRawFrame, err := defaultCodec.ConvertToRawFrame(targetNewFrame)
	if err != nil {
		return nil, nil, fmt.Errorf("could not convert modified target frame to raw frame: %w", err)
	}
	originInFlightFrame := &InFlightFrame{
		decodeContext: NewInitializedFrameDecodeContext(originRawFrame, originNewFrame, originNewStatementsQueryData),
		replacedTerms: originReplacedTerms}
	targetInFlightFrame := &InFlightFrame{
		decodeContext: NewInitializedFrameDecodeContext(targetRawFrame, targetNewFrame, targetNewStatementsQueryData),
		replacedTerms: targetReplacedTerms}
	return originInFlightFrame, targetInFlightFrame, nil
}
