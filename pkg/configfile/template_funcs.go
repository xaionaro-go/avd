package configfile

import (
	"reflect"
	"text/template"
)

var templateFuncs = template.FuncMap{
	"firstNonEmpty": func(args ...any) any {
		for _, arg := range args {
			v := reflect.ValueOf(arg)
			if v.IsValid() {
				return arg
			}
		}
		return nil
	},
}
