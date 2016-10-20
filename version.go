package fastly

import (
	"fmt"
	"net/http"
	"sort"
)

type VersionConfig config

type Version struct {
	ServiceID string `json:"service_id,omitempty"`
	Number    uint   `json:"number,omitempty"`
	Active    bool   `json:"active,omitempty"`

	Comment  string `json:"comment,omitempty"`
	Deployed bool   `json:"deployed,omitempty"`
	Locked   bool   `json:"locked,omitempty"`
	Staging  bool   `json:"staging,omitempty"`
	Testing  bool   `json:"testing,omitempty"`
	// TODO type these better
	Created string `json:"created_at"`
	Updated string `json:"updated_at"`
}

// versionsByNumber is a sortable list of versions.
type versionsByNumber []*Version

// Len, Swap, and Less implement the sortable interface.
func (s versionsByNumber) Len() int      { return len(s) }
func (s versionsByNumber) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s versionsByNumber) Less(i, j int) bool {
	return s[i].Number < s[j].Number
}

// List versions for a specific service.
func (c *VersionConfig) List(serviceID string) ([]*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version", serviceID)

	req, err := c.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	versions := new([]*Version)
	resp, err := c.client.Do(req, versions)
	if err != nil {
		return nil, resp, err
	}

	sort.Stable(versionsByNumber(*versions))

	return *versions, resp, nil
}

// Get fetches a specific version.
func (c *VersionConfig) Get(serviceID string, versionNumber uint) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d", serviceID, versionNumber)

	req, err := c.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	v := new(Version)
	resp, err := c.client.Do(req, v)
	if err != nil {
		return nil, resp, err
	}
	return v, resp, nil
}

// Validate validates a specific version.
func (c *VersionConfig) Validate(serviceID string, versionNumber uint) (*http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/validate", serviceID, versionNumber)

	req, err := c.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req, nil)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// Activate activates a specific version.
func (c *VersionConfig) Activate(serviceID string, versionNumber uint) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/activate", serviceID, versionNumber)

	req, err := c.client.NewRequest("PUT", u, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := c.client.Do(req, version)
	if err != nil {
		return nil, resp, err
	}
	return version, resp, nil
}

// Deactivate deactivates a specific version.
func (c *VersionConfig) Deactivate(serviceID string, versionNumber uint) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/deactivate", serviceID, versionNumber)

	req, err := c.client.NewRequest("PUT", u, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := c.client.Do(req, version)
	if err != nil {
		return nil, resp, err
	}
	return version, resp, nil
}

// Clone clones a specific version into a new version.
func (c *VersionConfig) Clone(serviceID string, versionNumber uint) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/clone", serviceID, versionNumber)

	req, err := c.client.NewRequest("PUT", u, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := c.client.Do(req, version)
	if err != nil {
		return nil, resp, err
	}
	return version, resp, nil
}

// Lock locks a specific version.
func (c *VersionConfig) Lock(serviceID string, versionNumber uint) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/lock", serviceID, versionNumber)

	req, err := c.client.NewRequest("PUT", u, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := c.client.Do(req, version)
	if err != nil {
		return nil, resp, err
	}
	return version, resp, nil
}

// Create a new version.
func (c *VersionConfig) Create(serviceID string) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version", serviceID)

	req, err := c.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := c.client.Do(req, version)
	if err != nil {
		return nil, resp, err
	}

	return version, resp, nil
}

// Update a version
func (c *VersionConfig) Update(serviceID string, versionNumber uint, version *Version) (*Version, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d", serviceID, versionNumber)

	req, err := c.client.NewJSONRequest("PUT", u, version)
	if err != nil {
		return nil, nil, err
	}

	b := new(Version)
	resp, err := c.client.Do(req, b)
	if err != nil {
		return nil, resp, err
	}

	return b, resp, nil
}
