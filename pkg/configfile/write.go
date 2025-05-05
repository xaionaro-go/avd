package configfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/xpath"
)

func Write[CFG io.WriterTo](
	ctx context.Context,
	cfgPath string,
	cfg CFG,
) (_err error) {
	logger.Debugf(ctx, "Write")
	defer func() { logger.Debugf(ctx, "/Write: %v", _err) }()

	cfgPathExpanded, err := xpath.Expand(cfgPath)
	if err != nil {
		return fmt.Errorf("unable to expand path '%s': %w", cfgPath, err)
	}
	cfgPath = cfgPathExpanded

	pathNew := cfgPath + ".new"
	f, err := os.OpenFile(pathNew, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0750)
	if err != nil {
		return fmt.Errorf("unable to open the data file '%s': %w", pathNew, err)
	}

	logger.Tracef(ctx, "cfg.WriteTo: %s", spew.Sdump(cfg))
	_, err = cfg.WriteTo(f)
	f.Close()
	if err != nil {
		return fmt.Errorf("unable to write data to file '%s': %w", pathNew, err)
	}

	backupDir := fmt.Sprintf("%s-backup", cfgPath)
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		logger.Errorf(ctx, "unable to create directory '%s'", backupDir)
	} else {
		now := time.Now()
		pathBackup := path.Join(
			backupDir,
			fmt.Sprintf(
				"%04d%02d%02d_%02d%02d.yaml",
				now.Year(), now.Month(), now.Day(),
				now.Hour(), now.Minute(),
			),
		)

		logger.Debugf(ctx, "backup path: '%s'", pathBackup)
		err = os.Rename(cfgPath, pathBackup)
		if err != nil {
			logger.Errorf(ctx, "cannot move '%s' to '%s': %w", cfgPath, pathBackup, err)
		}
	}

	err = os.Rename(pathNew, cfgPath)
	if err != nil {
		return fmt.Errorf("cannot move '%s' to '%s': %w", pathNew, cfgPath, err)
	}
	logger.Infof(ctx, "wrote to '%s' the config %#+v", cfgPath, cfg)

	return nil
}
