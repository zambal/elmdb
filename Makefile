MODULE = lmdb

DIALYZER = dialyzer
REBAR = rebar

.PHONY: compile clean

all: ebin priv compile

ebin:
	@mkdir -p $@

priv:
	@mkdir -p $@

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean
	@rm -f *~ */*~ erl_crash.dump
	@rm -rf ebin priv

xref:
	@$(REBAR) xref skip_deps=true

test: eunit

eunit: compile-for-eunit
	@$(REBAR) eunit skip_deps=true

eqc: compile-for-eqc
	@$(REBAR) eqc skip_deps=true

proper: compile-for-proper
	@echo "rebar does not implement a 'proper' command" && false

triq: compile-for-triq
	@$(REBAR) triq skip_deps=true

compile-for-eunit:
	@$(REBAR) compile eunit compile_only=true

compile-for-eqc:
	@$(REBAR) -D QC -D QC_EQC compile eqc compile_only=true

compile-for-eqcmini:
	@$(REBAR) -D QC -D QC_EQCMINI compile eqc compile_only=true

compile-for-proper:
	@$(REBAR) -D QC -D QC_PROPER compile eqc compile_only=true

compile-for-triq:
	@$(REBAR) -D QC -D QC_TRIQ compile triq compile_only=true

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa --apps kernel stdlib

analyze: compile
	@$(DIALYZER) --plt .$(TARGET).plt

repl:
	@$(ERL) exec erl -pa $PWD/ebin -pa +B
