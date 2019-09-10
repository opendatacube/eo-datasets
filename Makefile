
.PHONY: docker clean

docker:
	docker build -t opendatacube/eo-datasets:latest .

clean:
	echo "doing nothing"
