package redis

type QStatusOptions struct {
	Q        string `json:"q"`
	Consumer bool 	`json:"consumer"`
	Groups 	 bool 	`json:"groups"`
}

type QStatus struct {
	Info      Info     `json:"info"`
	Consumers []Consumer `json:"consumers"`
	Groups    []Group 	 `json:"groups"`
}

type Consumer struct {
	Name    string `json:"name"`
	Pending int64  `json:"pending"`
	Idle    int64  `json:"idle"`
}

type Info struct {
	Length          int64     `json:"length"`
	RadixTreeKeys   int64     `json:"radix_tree_keys"`
	RadixTreeNodes  int64     `json:"radix_tree_nodes"`
	LastGeneratedID string    `json:"last_generated_id"`
	Groups          int64     `json:"groups"`
	FirstEntry      Message   `json:"first_entry"`
	LastEntry       Message   `json:"last_entry"`
}

type Message struct {
	ID     string `json:"id"`
	Values map[string]interface{} `json:"values"`
}

type Group struct {
	Name            string `json:"name"`
	Consumers       int64  `json:"consumers"`
	Pending         int64  `json:"pending"`
	LastDeliveredID string `json:"last_delivered_id"`
}