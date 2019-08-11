package plugin

import (
	"errors"
	"fmt"
	"sync"

	"github.com/uber/cadence/common/log"
)

type Registry struct {
	logger  log.Logger
	mutex   sync.Mutex
	plugins map[string]Plugin
	used    bool
	// Providers by scope, name
	providers map[string]map[string]Provider
	// Providers by scope
	providersSlice map[string][]Provider
}

type ScopedRegistry struct {
	providers map[string]Provider
}

type Provider struct {
	PluginID string
	// Optional
	Name string
	// Optional. Empty means global
	Scope    string
	Function interface{}
}

type Plugin struct {
	ID          string
	Description string
	// key: name
	Providers []Provider
}

func NewRegistry() Registry {
	return Registry{
		plugins:   make(map[string]Plugin),
		providers: make(map[string]map[string]Provider),
	}
}

func (r *Registry) Register(plugin Plugin) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.used {
		return errors.New("Registry.Register called after usage");
	}
	if plugin.ID == "" {
		return fmt.Errorf("missing plugin ID: Description=%v", plugin.Description);
	}
	if _, ok := r.plugins[plugin.ID]; ok {
		return fmt.Errorf("failure registering plugin: duplicated plugin ID=%v", plugin.ID)
	}
	r.plugins[plugin.ID] = plugin
	for _, provider := range plugin.Providers {
		if byName, ok := r.providers[provider.Scope]; !ok {
			byName = make(map[string]Provider)
			byName[provider.Name] = provider
			r.providers[provider.Scope] = byName
			r.providersSlice[provider.Scope] = []Provider{provider}
		} else {
			if _, ok := byName[provider.Name]; ok {
				return fmt.Errorf("failure registering plugin: duplicated provider ID=%v, Scope=%v, Name=%v",
					plugin.ID, provider.Scope, provider.Name)
			}
			byName[provider.Name] = provider
			r.providersSlice[provider.Scope] = append(r.providersSlice[provider.Scope], provider)
		}
	}
	return nil
}

func (r *Registry) NewScopedRegistry(scope string) (ScopedRegistry, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.used = true
	if providers, ok := r.providers[scope]; ok {
		return ScopedRegistry{providers: providers}, nil
	} else {
		return ScopedRegistry{}, fmt.Errorf("NewScopedRegistry failure: no providers for scope %v", scope)
	}
}

func (r *Registry) GetAll(scope string) ([]Provider, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.used = true
	return r.providersSlice[scope], nil
}

func (r *ScopedRegistry) New(result interface{}, args ...interface{}) error {
	panic("not implemented")
}

func (r *ScopedRegistry) NewNamed(name string, result interface{}, args ...interface{}) error {
	panic("not implemented")
}

func (r *ScopedRegistry) GetNamesFor(result interface{}) ([]string, error) {
	panic("not implemented")
}

// result must be of []ResultType type.
func (r *ScopedRegistry) GetAll(result interface{}) error {
	panic("not implemented")
}
