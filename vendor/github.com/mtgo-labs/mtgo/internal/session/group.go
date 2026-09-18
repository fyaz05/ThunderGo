package session

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// errGroup wraps errgroup.Group so that goroutines receive the group context
// as a parameter instead of capturing it.
type errGroup struct {
	cancel context.CancelFunc
	eg     *errgroup.Group
	ctx    context.Context
}

func newErrGroup(parent context.Context) *errGroup {
	ctx, cancel := context.WithCancel(parent)
	eg, egCtx := errgroup.WithContext(ctx)
	return &errGroup{cancel: cancel, eg: eg, ctx: egCtx}
}

func (g *errGroup) Go(f func(ctx context.Context) error) {
	g.eg.Go(func() error { return f(g.ctx) })
}

func (g *errGroup) Cancel() { g.cancel() }

func (g *errGroup) Wait() error { return g.eg.Wait() }
