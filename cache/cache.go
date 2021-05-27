package cache

import (
	"fmt"
	"reflect"
	"sync"

	"log"

	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

const (
	updateEvent = "update"
	addEvent    = "add"
	deleteEvent = "delete"
	bufferSize  = 65536
)

// RowCache is a collections of Models hashed by UUID
type RowCache struct {
	cache map[string]model.Model
	mutex sync.RWMutex
}

// Row returns one model from the cache by UUID
func (r *RowCache) Row(uuid string) model.Model {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if row, ok := r.cache[uuid]; ok {
		return row.(model.Model)
	}
	return nil
}

// Create writes the provided content to the cache
func (r *RowCache) Create(uuid string, m model.Model) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.cache[uuid] = m
}

// Update replaces the content to the cache
func (r *RowCache) Update(uuid string, m model.Model) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.cache[uuid] = m
}

func (r *RowCache) Delete(uuid string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.cache, uuid)
}

// Rows returns a list of row UUIDs as strings
func (r *RowCache) Rows() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	var result []string
	for k := range r.cache {
		result = append(result, k)
	}
	return result
}

// Len returns the length of the cache
func (r *RowCache) Len() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.cache)
}

// NewRowCache creates a new row cache with the provided data
// if the data is nil, and empty RowCache will be created
func NewRowCache(data map[string]model.Model) *RowCache {
	if data == nil {
		data = make(map[string]model.Model)
	}
	return &RowCache{
		cache: data,
		mutex: sync.RWMutex{},
	}
}

// EventHandler can handle events when the contents of the cache changes
type EventHandler interface {
	OnAdd(table string, model model.Model)
	OnUpdate(table string, old model.Model, new model.Model)
	OnDelete(table string, model model.Model)
}

// EventHandlerFuncs is a wrapper for the EventHandler interface
// It allows a caller to only implement the functions they need
type EventHandlerFuncs struct {
	AddFunc    func(table string, model model.Model)
	UpdateFunc func(table string, old model.Model, new model.Model)
	DeleteFunc func(table string, model model.Model)
}

// OnAdd calls AddFunc if it is not nil
func (e *EventHandlerFuncs) OnAdd(table string, model model.Model) {
	if e.AddFunc != nil {
		e.AddFunc(table, model)
	}
}

// OnUpdate calls UpdateFunc if it is not nil
func (e *EventHandlerFuncs) OnUpdate(table string, old, new model.Model) {
	if e.UpdateFunc != nil {
		e.UpdateFunc(table, old, new)
	}
}

// OnDelete calls DeleteFunc if it is not nil
func (e *EventHandlerFuncs) OnDelete(table string, row model.Model) {
	if e.DeleteFunc != nil {
		e.DeleteFunc(table, row)
	}
}

// TableCache contains a collection of RowCaches, hashed by name,
// and an array of EventHandlers that respond to cache updates
type TableCache struct {
	cache          map[string]*RowCache
	cacheMutex     sync.RWMutex
	eventProcessor *eventProcessor
	mapper         *mapper.Mapper
	dbModel        *model.DBModel
}

// NewTableCache creates a new TableCache
func NewTableCache(schema *ovsdb.DatabaseSchema, dbModel *model.DBModel) (*TableCache, error) {
	if schema == nil || dbModel == nil {
		return nil, fmt.Errorf("tablecache without databasemodel cannot be populated")
	}
	eventProcessor := newEventProcessor(bufferSize)
	return &TableCache{
		cache:          make(map[string]*RowCache),
		eventProcessor: eventProcessor,
		mapper:         mapper.NewMapper(schema),
		dbModel:        dbModel,
	}, nil
}

// Mapper returns the mapper
func (t *TableCache) Mapper() *mapper.Mapper {
	return t.mapper
}

// DBModel returns the DBModel
func (t *TableCache) DBModel() *model.DBModel {
	return t.dbModel
}

// Table returns the a Table from the cache with a given name
func (t *TableCache) Table(name string) *RowCache {
	t.cacheMutex.RLock()
	defer t.cacheMutex.RUnlock()
	if table, ok := t.cache[name]; ok {
		return table
	}
	return nil
}

// Set write the provided RowCache to the provided table name in the cache
// if the provided cache is nil, we'll initialize a new one
// WARNING: Do not use Set outside of testing
func (t *TableCache) Set(name string, rc *RowCache) {
	if rc == nil {
		rc = NewRowCache(nil)
	}
	t.cacheMutex.Lock()
	defer t.cacheMutex.Unlock()
	t.cache[name] = rc
}

// Tables returns a list of table names that are in the cache
func (t *TableCache) Tables() []string {
	t.cacheMutex.RLock()
	defer t.cacheMutex.RUnlock()
	var result []string
	for k := range t.cache {
		result = append(result, k)
	}
	return result
}

// Update implements the update method of the NotificationHandler interface
// this populates the cache with new updates
func (t *TableCache) Update(context interface{}, tableUpdates ovsdb.TableUpdates) {
	if len(tableUpdates) == 0 {
		return
	}
	t.Populate(tableUpdates)
}

// Locked implements the locked method of the NotificationHandler interface
func (t *TableCache) Locked([]interface{}) {
}

// Stolen implements the stolen method of the NotificationHandler interface
func (t *TableCache) Stolen([]interface{}) {
}

// Echo implements the echo method of the NotificationHandler interface
func (t *TableCache) Echo([]interface{}) {
}

// Disconnected implements the disconnected method of the NotificationHandler interface
func (t *TableCache) Disconnected() {
}

// Populate adds data to the cache and places an event on the channel
func (t *TableCache) Populate(tableUpdates ovsdb.TableUpdates) {
	for table := range t.dbModel.Types() {
		updates, ok := tableUpdates[table]
		if !ok {
			continue
		}
		var tCache *RowCache
		if tCache = t.Table(table); tCache == nil {
			t.Set(table, nil)
		}
		tCache = t.Table(table)
		for uuid, row := range updates {
			if row.New != nil {
				newModel, err := t.CreateModel(table, row.New, uuid)
				if err != nil {
					panic(err)
				}
				if existing := tCache.Row(uuid); existing != nil {
					if !reflect.DeepEqual(newModel, existing) {
						tCache.Update(uuid, newModel)
						oldModel, err := t.CreateModel(table, row.Old, uuid)
						if err != nil {
							panic(err)
						}
						t.eventProcessor.AddEvent(updateEvent, table, oldModel, newModel)
					}
					// no diff
					continue
				}
				tCache.Create(uuid, newModel)
				t.eventProcessor.AddEvent(addEvent, table, nil, newModel)
				continue
			} else {
				oldModel, err := t.CreateModel(table, row.Old, uuid)
				if err != nil {
					panic(err)
				}
				tCache.Delete(uuid)
				t.eventProcessor.AddEvent(deleteEvent, table, oldModel, nil)
				continue
			}
		}
	}
}

// AddEventHandler registers the supplied EventHandler to recieve cache events
func (t *TableCache) AddEventHandler(handler EventHandler) {
	t.eventProcessor.AddEventHandler(handler)
}

// Run starts the event processing loop. It blocks until the channel is closed.
func (t *TableCache) Run(stopCh <-chan struct{}) {
	t.eventProcessor.Run(stopCh)
}

// event encapsualtes a cache event
type event struct {
	eventType string
	table     string
	old       model.Model
	new       model.Model
}

// eventProcessor handles the queueing and processing of cache events
type eventProcessor struct {
	events chan event
	// handlersMutex locks the handlers array when we add a handler or dispatch events
	// we don't need a RWMutex in this case as we only have one thread reading and the write
	// volume is very low (i.e only when AddEventHandler is called)
	handlersMutex sync.Mutex
	handlers      []EventHandler
}

func newEventProcessor(capacity int) *eventProcessor {
	return &eventProcessor{
		events:   make(chan event, capacity),
		handlers: []EventHandler{},
	}
}

// AddEventHandler registers the supplied EventHandler with the eventProcessor
// EventHandlers MUST process events quickly, for example, pushing them to a queue
// to be processed by the client. Long Running handler functions adversely affect
// other handlers and MAY cause loss of data if the channel buffer is full
func (e *eventProcessor) AddEventHandler(handler EventHandler) {
	e.handlersMutex.Lock()
	defer e.handlersMutex.Unlock()
	e.handlers = append(e.handlers, handler)
}

// AddEvent writes an event to the channel
func (e *eventProcessor) AddEvent(eventType string, table string, old model.Model, new model.Model) {
	// We don't need to check for error here since there
	// is only a single writer. RPC is run in blocking mode
	event := event{
		eventType: eventType,
		table:     table,
		old:       old,
		new:       new,
	}
	select {
	case e.events <- event:
		// noop
		return
	default:
		log.Print("dropping event because event buffer is full")
	}
}

// Run runs the eventProcessor loop.
// It will block until the stopCh has been closed
// Otherwise it will wait for events to arrive on the event channel
// Once recieved, it will dispatch the event to each registered handler
func (e *eventProcessor) Run(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case event := <-e.events:
			e.handlersMutex.Lock()
			for _, handler := range e.handlers {
				switch event.eventType {
				case addEvent:
					handler.OnAdd(event.table, event.new)
				case updateEvent:
					handler.OnUpdate(event.table, event.old, event.new)
				case deleteEvent:
					handler.OnDelete(event.table, event.old)
				}
			}
			e.handlersMutex.Unlock()
		}
	}
}

// createModel creates a new Model instance based on the Row information
func (t *TableCache) CreateModel(tableName string, row *ovsdb.Row, uuid string) (model.Model, error) {
	table := t.mapper.Schema.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	model, err := t.dbModel.NewModel(tableName)
	if err != nil {
		return nil, err
	}

	err = t.mapper.GetRowData(tableName, row, model)
	if err != nil {
		return nil, err
	}

	if uuid != "" {
		mapperInfo, err := mapper.NewMapperInfo(table, model)
		if err != nil {
			return nil, err
		}
		if err := mapperInfo.SetField("_uuid", uuid); err != nil {
			return nil, err
		}
	}

	return model, nil
}
