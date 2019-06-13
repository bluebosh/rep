package handlers

import (
	"encoding/json"
	"net/http"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auctioncellrep"

	opentracing "github.com/opentracing/opentracing-go"
)

type perform struct {
	rep auctioncellrep.AuctionCellClient
}

func (h *perform) ServeHTTP(w http.ResponseWriter, r *http.Request, logger lager.Logger) {
	logger = logger.Session("auction-perform-work")

	span := opentracing.GlobalTracer().StartSpan("rep.Perform")
	defer span.Finish()
	ctx := opentracing.ContextWithSpan(r.Context(), span)

	var work rep.Work
	err := json.NewDecoder(r.Body).Decode(&work)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		logger.Error("failed-to-unmarshal", err)
		return
	}

	failedWork, err := h.rep.Perform(ctx, logger, work)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed-to-perform-work", err)
		return
	}

	json.NewEncoder(w).Encode(failedWork)
}
