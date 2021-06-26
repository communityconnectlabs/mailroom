package mailroom

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/web"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/librato"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

// InitFunction is a function that will be called when mailroom starts
type InitFunction func(mr *Mailroom) error

var initFunctions = make([]InitFunction, 0)

// AddInitFunction adds an init function that will be called on startup
func AddInitFunction(initFunc InitFunction) {
	initFunctions = append(initFunctions, initFunc)
}

// TaskFunction is the function that will be called for a type of task
type TaskFunction func(ctx context.Context, mr *Mailroom, task *queue.Task) error

var taskFunctions = make(map[string]TaskFunction)

// AddTaskFunction adds an task function that will be called for a type of task
func AddTaskFunction(taskType string, taskFunc TaskFunction) {
	taskFunctions[taskType] = taskFunc
}

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	Config        *config.Config
	DB            *sqlx.DB
	RP            *redis.Pool
	ElasticClient *elastic.Client
	Storage       storage.Storage

	Quit      chan bool
	CTX       context.Context
	Cancel    context.CancelFunc
	WaitGroup *sync.WaitGroup

	batchForeman   *Foreman
	handlerForeman *Foreman

	webserver *web.Server
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *config.Config) *Mailroom {
	mr := &Mailroom{
		Config:    config,
		Quit:      make(chan bool),
		WaitGroup: &sync.WaitGroup{},
	}
	mr.CTX, mr.Cancel = context.WithCancel(context.Background())
	mr.batchForeman = NewForeman(mr, queue.BatchQueue, config.BatchWorkers)
	mr.handlerForeman = NewForeman(mr, queue.HandlerQueue, config.HandlerWorkers)

	return mr
}

// Start starts the mailroom service
func (mr *Mailroom) Start() error {
	log := logrus.WithFields(logrus.Fields{
		"state": "starting",
	})

	// parse and test our db config
	dbURL, err := url.Parse(mr.Config.DB)
	if err != nil {
		return fmt.Errorf("unable to parse DB URL '%s': %s", mr.Config.DB, err)
	}

	if dbURL.Scheme != "postgres" {
		return fmt.Errorf("invalid DB URL: '%s', only postgres is supported", mr.Config.DB)
	}

	// build our db
	db, err := sqlx.Open("postgres", mr.Config.DB)
	if err != nil {
		return fmt.Errorf("unable to open DB with config: '%s': %s", mr.Config.DB, err)
	}

	// configure our pool
	mr.DB = db
	mr.DB.SetMaxIdleConns(8)
	mr.DB.SetMaxOpenConns(mr.Config.DBPoolSize)
	mr.DB.SetConnMaxLifetime(time.Minute * 30)

	// try connecting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = mr.DB.PingContext(ctx)
	cancel()
	if err != nil {
		log.Error("db not reachable")
	} else {
		log.Info("db ok")
	}

	// parse and test our redis config
	redisURL, err := url.Parse(mr.Config.Redis)
	if err != nil {
		return fmt.Errorf("unable to parse Redis URL '%s': %s", mr.Config.Redis, err)
	}

	// create our pool
	redisPool := &redis.Pool{
		Wait:        true,              // makes callers wait for a connection
		MaxActive:   36,                // only open this many concurrent connections at once
		MaxIdle:     4,                 // only keep up to this many idle
		IdleTimeout: 240 * time.Second, // how long to wait before reaping a connection
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", fmt.Sprintf("%s", redisURL.Host))
			if err != nil {
				return nil, err
			}

			// send auth if required
			if redisURL.User != nil {
				pass, authRequired := redisURL.User.Password()
				if authRequired {
					if _, err := conn.Do("AUTH", pass); err != nil {
						conn.Close()
						return nil, err
					}
				}
			}

			// switch to the right DB
			_, err = conn.Do("SELECT", strings.TrimLeft(redisURL.Path, "/"))
			return conn, err
		},
	}
	mr.RP = redisPool

	// test our redis connection
	conn := redisPool.Get()
	defer conn.Close()
	_, err = conn.Do("PING")
	if err != nil {
		log.WithError(err).Error("redis not reachable")
	} else {
		log.Info("redis ok")
	}

	// create our storage (S3 or file system)
	if mr.Config.AWSAccessKeyID != "" {
		s3Client, err := storage.NewS3Client(&storage.S3Options{
			AWSAccessKeyID:     mr.Config.AWSAccessKeyID,
			AWSSecretAccessKey: mr.Config.AWSSecretAccessKey,
			Endpoint:           mr.Config.S3Endpoint,
			Region:             mr.Config.S3Region,
			DisableSSL:         mr.Config.S3DisableSSL,
			ForcePathStyle:     mr.Config.S3ForcePathStyle,
		})
		if err != nil {
			return err
		}
		mr.Storage = storage.NewS3(s3Client, mr.Config.S3MediaBucket)
	} else {
		mr.Storage = storage.NewFS("_storage")
	}

	// test our storage
	err = mr.Storage.Test()
	if err != nil {
		log.WithError(err).Error(mr.Storage.Name() + " storage not available")
	} else {
		log.Info(mr.Storage.Name() + " storage ok")
	}

	// initialize our elastic client
	mr.ElasticClient, err = newElasticClient(mr.Config.Elastic)
	if err != nil {
		log.WithError(err).Error("unable to connect to elastic, check configuration")
	} else {
		log.Info("elastic ok")
	}

	// warn if we won't be doing FCM syncing
	if config.Mailroom.FCMKey == "" {
		logrus.Error("fcm not configured, no syncing of android channels")
	}

	for _, initFunc := range initFunctions {
		initFunc(mr)
	}

	// if we have a librato token, configure it
	if mr.Config.LibratoToken != "" {
		host, _ := os.Hostname()
		librato.Configure(mr.Config.LibratoUsername, mr.Config.LibratoToken, host, time.Second, mr.WaitGroup)
		librato.Start()
	}

	// init our foremen and start it
	mr.batchForeman.Start()
	mr.handlerForeman.Start()

	// start our web server
	mr.webserver = web.NewServer(mr.CTX, mr.Config, mr.DB, mr.RP, mr.Storage, mr.ElasticClient, mr.WaitGroup)
	mr.webserver.Start()

	logrus.Info("mailroom started")

	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	logrus.Info("mailroom stopping")
	mr.batchForeman.Stop()
	mr.handlerForeman.Stop()
	librato.Stop()
	close(mr.Quit)
	mr.Cancel()

	// stop our web server
	mr.webserver.Stop()

	mr.WaitGroup.Wait()
	mr.ElasticClient.Stop()
	logrus.Info("mailroom stopped")
	return nil
}

func newElasticClient(url string) (*elastic.Client, error) {
	// enable retrying
	backoff := elastic.NewSimpleBackoff(500, 1000, 2000)
	backoff.Jitter(true)
	retrier := elastic.NewBackoffRetrier(backoff)

	return elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetRetrier(retrier),
	)
}
