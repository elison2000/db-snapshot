package model

import "gorm.io/gorm"

type Capturer interface {
	Init() error
	Capture(*gorm.DB)
	Close()
}
