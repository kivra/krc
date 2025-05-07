suite=$(if $(SUITE), suite=$(SUITE), )

.PHONY:	all deps check test clean

all: deps
	rebar3 compile

deps:
	rebar3 get-deps

docs:
	rebar3 doc

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
	$(RM) doc/*

# eof
