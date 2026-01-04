// read.go provides functions to read configuration files.

// Package configfile provides functions to read and write configuration files.
package configfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"text/template"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/xpath"
)

type Config interface {
	io.Reader
	io.WriterTo
}

func Read[CFG Config](
	ctx context.Context,
	cfgPath string,
	cfg CFG,
	info any,
) (_ret bool, _err error) {
	logger.Debugf(ctx, "Read(ctx, '%s', &cfg)", cfgPath)
	defer func() { logger.Debugf(ctx, "/Read(ctx, '%s', &cfg): %v %v", cfgPath, _ret, _err) }()

	cfgPathExpanded, err := xpath.Expand(cfgPath)
	if err != nil {
		return false, fmt.Errorf("unable to expand path '%s': %w", cfgPath, err)
	}
	cfgPath = cfgPathExpanded

	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		return false, nil
	}

	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return false, fmt.Errorf("unable to read file '%s': %w", cfgPath, err)
	}

	tmpl, err := template.New("").Funcs(templateFuncs).Parse(string(b))
	if err != nil {
		return false, fmt.Errorf("unable to parse config file '%s' as template: %w", cfgPath, err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, info)
	if err != nil {
		return false, fmt.Errorf("unable to execute config file '%s' as template: %w", cfgPath, err)
	}

	unparsed := buf.Bytes()
	logger.Tracef(ctx, "unparsed config == %s", unparsed)
	_, err = cfg.Read(unparsed)

	var cfgSerialized bytes.Buffer
	if _, _err := cfg.WriteTo(&cfgSerialized); _err != nil {
		logger.Error(ctx, _err)
	} else {
		logger.Tracef(ctx, "parsed config == %s", cfgSerialized.String())
	}

	return true, err
}
