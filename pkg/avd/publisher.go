package avd

import (
	"github.com/xaionaro-go/avpipeline/router"
)

type Publisher interface {
	router.Publisher[RouteCustomData]
}
type Publishers []Publisher
