[![GoDoc](https://godoc.org/github.com/Applifier/go-bloomd?status.svg)](http://godoc.org/github.com/Applifier/go-bloomd)

# go-bloomd

Bloomd client (with connection pool) for Go


```sh
$ go get -u github.com/Applifier/go-bloomd
```

# Example

```go
c, _ := bloomd.NewFromAddr("localhost:8673")
f, _ := c.CreateFilter(Filter{
	Name: "somefilter",
})

f.Set("foobar")
found, _ := f.Check("foobar")
```