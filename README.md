[![GoDoc](https://godoc.org/github.com/Applifier/go-bloomd?status.svg)](http://godoc.org/github.com/Applifier/go-bloomd)

# go-bloomd

Bloomd client (with connection pool) for Go


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