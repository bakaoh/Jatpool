# Jatpool #

## Description ##

thrift client pool for java

## Using ##

* Create a pool and get client

```
Jatpool<Iface> pool = Jatpool.getInstance(Iface.class, "host_port_pair");
pool.cli();

// call thrift
```


