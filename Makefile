suite=$(if $(SUITE), suite=$(SUITE), )

.PHONY:	all ci deps check test clean

all: deps
	rebar3 compile

ci: clean xref dialyzer up test down
.NOTPARALLEL: ci up test down

deps:
	rebar3 get-deps

dialyzer:
	rebar3 dialyzer

test:
	rebar3 eunit $(suite) skip_deps=true

up:
	docker compose up --detach --remove-orphans --wait

down:
	docker compose down --remove-orphans

xref:
	rebar3 xref

clean:
	rebar3 clean

# eof
