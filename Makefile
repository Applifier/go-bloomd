test:
	docker-compose run test
	docker-compose down

coveralls:
	go tool cover -func=coverage.out
	goveralls -coverprofile=coverage.out -repotoken $$COVERALLS_TOKEN -package bloomd

.PHONY: test