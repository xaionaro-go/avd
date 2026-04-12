package server

// sentinelConsumer is a lightweight consumer registered on a route to
// keep it active during on-demand transcoding. The route's idle-timer
// sees at least one consumer and does not shut down the pipeline.
//
// Value equality: two sentinelConsumer values with the same underlying
// string compare equal via ==, which is how router.RemoveConsumer
// matches consumers. This means ActivateRoute/DeactivateRoute are
// stateless — the server reconstructs the same sentinel from the
// route path without storing references.
type sentinelConsumer string

func (c sentinelConsumer) String() string {
	return string(c)
}

func sentinelConsumerForRoute(routePath string) sentinelConsumer {
	return sentinelConsumer("on-demand:" + routePath)
}
