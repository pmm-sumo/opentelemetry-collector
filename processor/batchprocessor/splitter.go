package batchprocessor

import "go.opentelemetry.io/collector/consumer/pdata"

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

type traceSplitter struct {
	splits        []pdata.Traces
	cur           *pdata.Traces
	limit         int
	curSpansCount int
}

func (ts *traceSplitter) startNewBatch() {
	ts.splits = append(ts.splits, pdata.NewTraces())
	ts.cur = &ts.splits[len(ts.splits)-1]
	ts.curSpansCount = 0
}

func (ts *traceSplitter) startNewBatchFromResource(oldToRs pdata.ResourceSpans) pdata.ResourceSpans {
	ts.startNewBatch()

	newToRss := ts.cur.ResourceSpans()
	newToRss.Resize(1)
	newToRs := newToRss.At(0)
	oldToRs.Resource().CopyTo(newToRs.Resource())

	return newToRs
}

func (ts *traceSplitter) startNewBatchFromResourceAndSpans(oldToRs pdata.ResourceSpans, oldToIls pdata.InstrumentationLibrarySpans) (pdata.ResourceSpans, pdata.InstrumentationLibrarySpans) {
	newToRs := ts.startNewBatchFromResource(oldToRs)

	newToRs.InstrumentationLibrarySpans().Resize(1)
	oldToIls.InstrumentationLibrary().CopyTo(newToRs.InstrumentationLibrarySpans().At(0).InstrumentationLibrary())

	return newToRs, newToRs.InstrumentationLibrarySpans().At(0)
}

func newTraceSplitter(limit int) *traceSplitter {
	splits := make([]pdata.Traces, 1)
	splits[0] = pdata.NewTraces()
	return &traceSplitter{
		splits:        splits,
		cur:           &splits[0],
		limit:         limit,
		curSpansCount: 0,
	}
}

func ilssSpanCount(ilss pdata.InstrumentationLibrarySpansSlice) int {
	len := 0
	for j := 0; j < ilss.Len(); j++ {
		if ilss.At(j).IsNil() {
			continue
		}
		len = +ilss.At(j).Spans().Len()
	}
	return len
}

func (ts *traceSplitter) addResourceSpansSlice(fromRss pdata.ResourceSpansSlice) {
	for i := 0; i < fromRss.Len(); i++ {
		fromRs := fromRss.At(i)
		ts.addResourceSpan(fromRs)
	}
}

func (ts *traceSplitter) addResourceSpan(fromRs pdata.ResourceSpans) {
	fromIlss := fromRs.InstrumentationLibrarySpans()

	// Start new batch if it's on the limit (TODO: do it also when it's close to the limit, to do it coarse-granularly)
	if ts.curSpansCount >= ts.limit {
		ts.startNewBatch()
	}

	ts.cur.ResourceSpans().Resize(ts.cur.ResourceSpans().Len() + 1)
	toRs := ts.cur.ResourceSpans().At(ts.cur.ResourceSpans().Len() - 1)
	fromRs.Resource().CopyTo(toRs.Resource())

	if ilssSpanCount(fromIlss)+ts.curSpansCount < ts.limit {
		ts.curSpansCount += ilssSpanCount(fromIlss)
		fromRs.CopyTo(toRs)
	} else {
		ts.addInstrumentationLibrarySpansSlice(fromIlss, toRs)
	}
}

func (ts *traceSplitter) addInstrumentationLibrarySpansSlice(fromIlss pdata.InstrumentationLibrarySpansSlice, toRs pdata.ResourceSpans) {
	// toRs has already the resource copied, we are going reuse it if more ResourceSpans are needed

	toIlss := toRs.InstrumentationLibrarySpans()

	for j := 0; j < fromIlss.Len(); j++ {
		if ts.curSpansCount >= ts.limit {
			toRs = ts.startNewBatchFromResource(toRs)
			toIlss = toRs.InstrumentationLibrarySpans()
		}

		fromIls := fromIlss.At(j)
		if fromIls.IsNil() {
			continue
		}

		// Make space for one more library spans struct
		toIlss.Resize(toIlss.Len() + 1)
		toIls := toIlss.At(toIlss.Len() - 1)

		// Copy instrumentation library data
		fromIls.InstrumentationLibrary().CopyTo(toIls.InstrumentationLibrary())

		if fromIls.Spans().Len()+ts.curSpansCount < ts.limit {
			ts.curSpansCount += fromIls.Spans().Len()
			fromIls.Spans().MoveAndAppendTo(toIls.Spans())
		} else {
			// We might copy only limited number of spans...
			index := 0
			for index < fromIls.Spans().Len() {
				if ts.curSpansCount >= ts.limit {
					toRs, toIls = ts.startNewBatchFromResourceAndSpans(toRs, toIls)
				}

				index = ts.addSpans(fromIls, toRs, toIls, index)
			}
		}
	}
}

// addSpans add specific spans up the the limit; it returns the current position of the index - if it's equal to length, everything was consumed
func (ts *traceSplitter) addSpans(fromIls pdata.InstrumentationLibrarySpans, toRs pdata.ResourceSpans, toIls pdata.InstrumentationLibrarySpans, startIndex int) int {
	desiredCount := min(ts.limit-ts.curSpansCount, fromIls.Spans().Len()-startIndex)
	toIls.Spans().Resize(desiredCount)
	fromSpanIndex := startIndex

	for k := 0; k < desiredCount; k++ {
		fromIls.Spans().At(fromSpanIndex).CopyTo(toIls.Spans().At(k))
		fromSpanIndex += 1
	}
	ts.curSpansCount += desiredCount

	return fromSpanIndex
}

// SplitItems takes the spans and splits then into batches of no more than limitPerBatch spans each
func SplitItems(data pdata.Traces, limitPerBatch int) []pdata.Traces {
	ts := newTraceSplitter(limitPerBatch)
	ts.addResourceSpansSlice(data.ResourceSpans())
	return ts.splits
}
