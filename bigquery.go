package fastly

import (
	"fmt"
	"net/http"
	"sort"
)

type BigQueryConfig config

// https://docs.fastly.com/api/logging#logging_bigquery
type BigQuery struct {
	ServiceID string `json:"service_id,omitempty"`
	Version   uint   `json:"version,string,omitempty"`

	Name              string `json:"name,omitempty"`
	Format            string `json:"format"`
	User              string `json:"user"`
	SecretKey         string `json:"secret_key,omitempty"`
	ProjectID         string `json:"project_id"`
	Dataset           string `json:"dataset"`
	Table             string `json:"table"`
	TemplateSuffix    string `json:"template_suffix,omitempty"`
	ResponseCondition string `json:"response_condition"`
}

// s3sByName is a sortable list of BigQuerys.
type bqsByName []*BigQuery

// Len, Swap, and Less implement the sortable interface.
func (s bqsByName) Len() int      { return len(s) }
func (s bqsByName) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s bqsByName) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

// List bqs for a specific service and version.
func (c *BigQueryConfig) List(serviceID string, version uint) ([]*BigQuery, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/logging/bigquery", serviceID, version)

	req, err := c.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	bqs := new([]*BigQuery)
	resp, err := c.client.Do(req, bqs)
	if err != nil {
		return nil, resp, err
	}

	sort.Stable(bqsByName(*bqs))

	return *bqs, resp, nil
}

// Get fetches a specific bq by name.
func (c *BigQueryConfig) Get(serviceID string, version uint, name string) (*BigQuery, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/logging/bigquery/%s", serviceID, version, name)

	req, err := c.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	bq := new(BigQuery)
	resp, err := c.client.Do(req, bq)
	if err != nil {
		return nil, resp, err
	}
	return bq, resp, nil
}

// Create a new bq.
func (c *BigQueryConfig) Create(serviceID string, version uint, bq *BigQuery) (*BigQuery, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/logging/bigquery", serviceID, version)

	req, err := c.client.NewJSONRequest("POST", u, bq)
	if err != nil {
		return nil, nil, err
	}

	b := new(BigQuery)
	resp, err := c.client.Do(req, b)
	if err != nil {
		return nil, resp, err
	}

	return b, resp, nil
}

// Update a bq
func (c *BigQueryConfig) Update(serviceID string, version uint, name string, bq *BigQuery) (*BigQuery, *http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/logging/bigquery/%s", serviceID, version, name)

	req, err := c.client.NewJSONRequest("PUT", u, bq)
	if err != nil {
		return nil, nil, err
	}

	b := new(BigQuery)
	resp, err := c.client.Do(req, b)
	if err != nil {
		return nil, resp, err
	}

	return b, resp, nil
}

// Delete a bq
func (c *BigQueryConfig) Delete(serviceID string, version uint, name string) (*http.Response, error) {
	u := fmt.Sprintf("/service/%s/version/%d/logging/bigquery/%s", serviceID, version, name)

	req, err := c.client.NewRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}
