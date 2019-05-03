package mapreduce

type KeyValue struct { Key, Value string }

func (kv *KeyValue) toStr() string { return kv.Key + " " + kv.Value }
