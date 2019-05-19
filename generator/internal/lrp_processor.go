package internal

import (
	"context"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/models"
	loggingclient "code.cloudfoundry.org/diego-logging-client"
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
)

type lrpContainer struct {
	*models.ActualLRPKey
	*models.ActualLRPInstanceKey
	executor.Container
}

func newLRPContainer(lrpKey *models.ActualLRPKey, instanceKey *models.ActualLRPInstanceKey, container executor.Container) *lrpContainer {
	return &lrpContainer{
		ActualLRPKey:         lrpKey,
		ActualLRPInstanceKey: instanceKey,
		Container:            container,
	}
}

//go:generate counterfeiter -o fake_internal/fake_lrp_processor.go lrp_processor.go LRPProcessor

type LRPProcessor interface {
	Process(context.Context, lager.Logger, executor.Container)
}

type lrpProcessor struct {
	evacuationReporter  evacuation_context.EvacuationReporter
	ordinaryProcessor   LRPProcessor
	evacuationProcessor LRPProcessor
}

func NewLRPProcessor(
	bbsClient bbs.InternalClient,
	containerDelegate ContainerDelegate,
	metronClient loggingclient.IngressClient,
	cellID string,
	evacuationReporter evacuation_context.EvacuationReporter,
	evacuationTTLInSeconds uint64,
) LRPProcessor {
	ordinaryProcessor := newOrdinaryLRPProcessor(bbsClient, containerDelegate, cellID)
	evacuationProcessor := newEvacuationLRPProcessor(bbsClient, containerDelegate, metronClient, cellID, evacuationTTLInSeconds)
	return &lrpProcessor{
		evacuationReporter:  evacuationReporter,
		ordinaryProcessor:   ordinaryProcessor,
		evacuationProcessor: evacuationProcessor,
	}
}

func (p *lrpProcessor) Process(ctx context.Context, logger lager.Logger, container executor.Container) {
	if p.evacuationReporter.Evacuating() {
		p.evacuationProcessor.Process(ctx, logger, container)
	} else {
		p.ordinaryProcessor.Process(ctx, logger, container)
	}
}
