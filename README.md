[![Build Status](https://travis-ci.org/Applifier/go-bloomd.svg?branch=master)](https://travis-ci.org/Applifier/go-bloomd)
[![Coverage Status](https://coveralls.io/repos/github/Applifier/go-bloomd/badge.svg?branch=master)](https://coveralls.io/github/Applifier/go-bloomd?branch=master)
[![GoDoc](https://godoc.org/github.com/Applifier/go-bloomd?status.svg)](http://godoc.org/github.com/Applifier/go-bloomd)

# go-bloomd

Bloomd (https://github.com/armon/bloomd) client (with connection pool) for Go


```sh
$ go get -u github.com/Applifier/go-bloomd
```

# Example

```go
c, _ := bloomd.NewFromAddr("localhost:8673")
defer c.Close()

f, _ := c.CreateFilter(Filter{
	Name: "somefilter",
})

f.Set("foobar")
found, _ := f.Check("foobar")
```

## Client pool

Client pools can be used to maintain a pool of persistent connections to bloomd server

```go
p, _ := bloomd.NewPoolFromAddr(5, 10, "localhost:8673")
c, _ := p.Get()
defer c.Close() // Return client back to pool

f, _ := c.CreateFilter(Filter{
	Name: "somefilter",
})

f.Set("foobar")
found, _ := f.Check("foobar")
```