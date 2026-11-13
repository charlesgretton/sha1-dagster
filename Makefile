PYTHON ?= python3

# ---- the release checks and the virtual proceedings ------------------------
# every check in the repository; all must pass before any result is trusted
selftest: check-files all
	$(PYTHON) tools/selftest.py

# the virtual proceedings (self-contained; three passes for references)
proceedings:
	cd proceedings && for i in 1 2 3; do pdflatex -interaction=nonstopmode -halt-on-error sha1_dagster_proceedings.tex > /dev/null || exit 1; done
	@echo "built proceedings/sha1_dagster_proceedings.pdf"

# reproducibility manifest: checksums of every tracked artefact plus toolchain
manifest:
	@echo "# sha1-dagster manifest"                        >  MANIFEST.sha256
	@echo "# version: $$(cat VERSION)"                    >> MANIFEST.sha256
	@echo "# generated: $$(date -u +%Y-%m-%dT%H:%M:%SZ)"  >> MANIFEST.sha256
	@echo "# python:   $$($(PYTHON) --version)"           >> MANIFEST.sha256
	@echo "# platform: $$(uname -srm)"                     >> MANIFEST.sha256
	@find . -type f ! -name MANIFEST.sha256 ! -path './.git/*' ! -path './bin/*' ! -path './build-modern/*' \
	      ! -name '*.pyc' ! -name '*.aux' ! -name '*.log' ! -name '*.out' ! -name '*.toc' -print0 \
	  | sort -z | xargs -0 sha256sum                    >> MANIFEST.sha256
	@echo "wrote MANIFEST.sha256 ($$(grep -c '^[0-9a-f]' MANIFEST.sha256) files)"

# refuse to proceed if any expected artefact is missing
check-files:
	@for f in proceedings/sha1_dagster_proceedings.tex proceedings/sha1_dagster_proceedings.pdf \
	          third_party/sha1-sat/main_noboost.cc third_party/sha1-sat/COPYING patches/dagster_bounded_v3.py \
	          tools/selftest.py tools/verify_preimage.py tools/compositional_race.py tools/report_stats.py \
	          evidence/derived/runs.csv evidence/derived/summary.csv evidence/checksums.txt evidence/provenance.txt \
	          evidence/scripts/derive.py scripts/runsheet_demo.sh scripts/phaseA_regression.sh \
	          VERSION README.md LICENSE LICENSE-CC-BY-4.0.md LICENSING.md REUSE.toml; do \
	    test -f $$f || { echo "MISSING: $$f"; exit 1; }; done
	@echo "all expected artefacts present"

clean:
	rm -rf bin build-modern
	rm -f proceedings/*.aux proceedings/*.log proceedings/*.out proceedings/*.toc
	find . -name __pycache__ -prune -exec rm -rf {} +

.PHONY: selftest proceedings manifest check-files clean

# ---- the instance generator: third_party/sha1-sat (GPL-3.0-or-later) --------
# Builds the two sha1-sat C++ tools. sha1-sat is Copyright (C) 2011-2012, 2021
# Vegard Nossum; see third_party/sha1-sat/COPYING and MODIFICATIONS.md. Only a
# C++11 compiler is needed (main_noboost.cc is the Boost-free variant).
CXX      ?= g++
CXXFLAGS ?= -O2 -w -std=c++11
SHA1SAT  := third_party/sha1-sat

all: bin/sha1gen bin/verify-preimage

bin:
	mkdir -p bin

bin/sha1gen: $(SHA1SAT)/main_noboost.cc $(SHA1SAT)/format.hh | bin
	$(CXX) $(CXXFLAGS) -I$(SHA1SAT) -o $@ $(SHA1SAT)/main_noboost.cc

bin/verify-preimage: $(SHA1SAT)/verify-preimage.cc | bin
	$(CXX) $(CXXFLAGS) -o $@ $(SHA1SAT)/verify-preimage.cc

.PHONY: all

# ---- portfolio engines (need your Dagster source tree) ----------------------
#   make engines DAGSTER_SRC=/path/to/dagster-main/dagster
# Honours CPPFLAGS / LDFLAGS / MPICXX, so any include or library fix you needed
# for Dagster's own build (glog prefix, arch include dir, Cray CC) applies here:
#   make engines DAGSTER_SRC=... CPPFLAGS="-I/usr/include/x86_64-linux-gnu"
#   make engines DAGSTER_SRC=... MPICXX=CC
DAGSTER_SRC ?= ../dagster-main/dagster
DAGSTER_ABS := $(abspath $(DAGSTER_SRC))
MPICXX      ?= mpic++
ENGINES      = portfolio/engines
ENGINE_FLAGS = -O3 -std=c++17 -DNDEBUG -fno-strict-aliasing

engines: check-dagster $(ENGINES)/minisat $(ENGINES)/tinisat_seeded

check-dagster:
	@test -f "$(DAGSTER_ABS)/SatSolver.cpp" -a -f "$(DAGSTER_ABS)/minisat/core/Solver.cc" || { \
	  echo "ERROR: DAGSTER_SRC=$(DAGSTER_SRC) is not the Dagster source directory"; \
	  echo "       (expected $(DAGSTER_ABS)/SatSolver.cpp and minisat/core/Solver.cc)"; \
	  echo "       run: make engines DAGSTER_SRC=/path/to/dagster-main/dagster"; exit 2; }

# MiniSat exactly as bundled with Dagster (its seed needs -rnd-init)
$(ENGINES)/minisat: | check-dagster
	$(CXX) $(CPPFLAGS) -O3 -DNDEBUG -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS \
	  -I$(DAGSTER_ABS)/minisat \
	  $(DAGSTER_ABS)/minisat/core/Main.cc $(DAGSTER_ABS)/minisat/core/Solver.cc \
	  $(DAGSTER_ABS)/minisat/utils/Options.cc $(DAGSTER_ABS)/minisat/utils/System.cc \
	  $(LDFLAGS) -lz -o $@

# Dagster's TiniSAT with a controlled seed and 10/20 exit codes.
# Same include roots as Dagster's Makefile (-I. -Iminisat, run from dagster/).
$(ENGINES)/tinisat_seeded: $(ENGINES)/tinisat_seeded.cc | check-dagster
	$(MPICXX) $(CPPFLAGS) $(ENGINE_FLAGS) \
	  -I$(DAGSTER_ABS) -I$(DAGSTER_ABS)/minisat \
	  $< $(LDFLAGS) -lglog -lstdc++fs -o $@

engines-clean:
	rm -f $(ENGINES)/minisat $(ENGINES)/tinisat_seeded

.PHONY: engines check-dagster engines-clean

# ---- modern solvers (Kissat, CaDiCaL), pinned to the commits validated -------
#   make modern-engines                 downloads the pinned source from GitHub
#   make modern-engines KISSAT_SRC=... CADICAL_SRC=...   build from local trees
KISSAT_COMMIT  = 8af8e56f174b778aef3aa45af9f739b2a5f492c2
CADICAL_COMMIT = c60730422e758ef1cebe7aeddf2dda31c996bf04
MODERN_BUILD   = build-modern

modern-engines: $(ENGINES)/kissat $(ENGINES)/cadical

$(ENGINES)/kissat:
	@mkdir -p $(MODERN_BUILD)
	@if [ -n "$(KISSAT_SRC)" ]; then rm -rf $(MODERN_BUILD)/kissat && cp -r "$(KISSAT_SRC)" $(MODERN_BUILD)/kissat; \
	 else rm -rf $(MODERN_BUILD)/kissat* && curl -fsSL https://codeload.github.com/arminbiere/kissat/tar.gz/$(KISSAT_COMMIT) | tar xz -C $(MODERN_BUILD) \
	      && mv $(MODERN_BUILD)/kissat-$(KISSAT_COMMIT) $(MODERN_BUILD)/kissat; fi
	cd $(MODERN_BUILD)/kissat && ./configure > configure.log && $(MAKE) > make.log
	cp $(MODERN_BUILD)/kissat/build/kissat $@

$(ENGINES)/cadical:
	@mkdir -p $(MODERN_BUILD)
	@if [ -n "$(CADICAL_SRC)" ]; then rm -rf $(MODERN_BUILD)/cadical && cp -r "$(CADICAL_SRC)" $(MODERN_BUILD)/cadical; \
	 else rm -rf $(MODERN_BUILD)/cadical* && curl -fsSL https://codeload.github.com/arminbiere/cadical/tar.gz/$(CADICAL_COMMIT) | tar xz -C $(MODERN_BUILD) \
	      && mv $(MODERN_BUILD)/cadical-$(CADICAL_COMMIT) $(MODERN_BUILD)/cadical; fi
	cd $(MODERN_BUILD)/cadical && ./configure > configure.log && $(MAKE) > make.log
	cp $(MODERN_BUILD)/cadical/build/cadical $@

.PHONY: modern-engines
