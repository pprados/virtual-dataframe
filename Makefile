#!/usr/bin/env make

# SNIPPET Le shebang précédant permet de creer des alias des cibles du Makefile.
# Il faut que le Makefile soit executable
# 	chmod u+x Makefile
# 	git update-index --chmod=+x Makefile
# Puis, par exemple
# 	ln -s Makefile configure
# 	ln -s Makefile test
# 	./configure		# Execute make configure
# 	./test 			# Execute make test
# Attention, il n'est pas possible de passer les paramètres aux scripts

# ---------------------------------------------------------------------------------------
# SNIPPET pour vérifier la version du programme `make`.
# WARNING: Use make >4.0
ifeq ($(shell echo "$(shell echo $(MAKE_VERSION) | sed 's@^[^0-9]*\([0-9]\+\).*@\1@' ) >= 4" | bc -l),0)
$(error Bad make version, please install make >= 4)
endif
# ---------------------------------------------------------------------------------------
# SNIPPET pour changer le mode de gestion du Makefile.
# Avec ces trois paramètres, toutes les lignes d'une recette sont invoquées dans le même shell.
# Ainsi, il n'est pas nécessaire d'ajouter des '&&' ou des '\' pour regrouper les lignes.
# Comme Make affiche l'intégralité du block de la recette avant de l'exécuter, il n'est
# pas toujours facile de savoir quel est la ligne en échec.
# Je vous conseille dans ce cas d'ajouter au début de la recette 'set -x'
# Attention : il faut une version > 4 de  `make` (`make -v`).
# Les versions CentOS d'Amazone ont une version 3.82.
# Utilisez `conda install -n $(VENV_AWS) make>=4 -y`
# WARNING: Use make >4.0
SHELL=/bin/bash
.SHELLFLAGS = -e -c
.ONESHELL:


# ---------------------------------------------------------------------------------------
# SNIPPET pour injecter les variables de .env.
ifneq (,$(wildcard .env))
include .env
endif


# ---------------------------------------------------------------------------------------
# SNIPPET pour détecter l'OS d'exécution.
ifeq ($(OS),Windows_NT)
    OS := Windows
    EXE:=.exe
    SUDO?=
else
    OS := $(shell sh -c 'uname 2>/dev/null || echo Unknown')
    EXE:=
    SUDO?=
endif


# ---------------------------------------------------------------------------------------
# SNIPPET pour détecter la présence d'un GPU afin de modifier le nom du projet
# et ses dépendances si nécessaire.
ifndef USE_GPU
# https://stackoverflow.com/questions/66611439/how-to-check-if-nvidia-gpu-is-available-using-bash-script
ifneq ("$(wildcard /proc/driver/nvidia)","")
USE_GPU:=-gpu
else ifdef CUDA_HOME
USE_GPU:=-gpu
endif
endif
ifdef USE_GPU
CUDA_VER=$(shell nvidia-smi | awk -F"CUDA Version:" 'NR==3{split($$2,a," ");print a[1]}')
# BUG avec 11.7. Cudf ne s'install plus ?
endif

# ---------------------------------------------------------------------------------------
# SNIPPET pour identifier le nombre de processeur
NPROC?=$(shell nproc)

# ---------------------------------------------------------------------------------------
# SNIPPET pour pouvoir lancer un browser avec un fichier local
define BROWSER
	python $(PYTHON_ARGS) -c '
	import os, sys, webbrowser
	from urllib.request import pathname2url

	webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])), autoraise=True)
	sys.exit(0)
	'
endef

# ---------------------------------------------------------------------------------------
# SNIPPET pour supprimer le parallelisme pour certaines cibles
# par exemple pour release
ifneq ($(filter configure release clean functional-test% upgrade-%,$(MAKECMDGOALS)),)
.NOTPARALLEL:
endif
#

# ---------------------------------------------------------------------------------------
# SNIPPET pour récupérer les séquences de caractères pour les couleurs
# A utiliser avec un
# echo -e "Use '$(cyan)make$(normal)' ..."
# Si vous n'utilisez pas ce snippet, les variables de couleurs non initialisés
# sont simplement ignorées.
ifneq ($(TERM),)
normal:=$(shell tput sgr0)
bold:=$(shell tput bold)
red:=$(shell tput setaf 1)
green:=$(shell tput setaf 2)
yellow:=$(shell tput setaf 3)
blue:=$(shell tput setaf 4)
purple:=$(shell tput setaf 5)
cyan:=$(shell tput setaf 6)
white:=$(shell tput setaf 7)
gray:=$(shell tput setaf 8)
endif

# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer le projet, le virtualenv conda et le kernel.
# Par convention, les noms du projet, de l'environnement Conda ou le Kernel Jupyter
# correspondent au nom du répertoire du projet.
# Il est possible de modifier cela en valorisant les variables VENV, KERNEL, et/ou PRJ.
# avant le lancement du Makefile (`VENV=cntk_p36 make`)
PRJ:=$(shell basename "$(shell pwd)")
VENV ?= $(PRJ)
KERNEL ?=$(VENV)
REMOTE_GIT_URL?=$(shell git remote get-url origin)
PRJ_URL=$(REMOTE_GIT_URL:.git=)
PRJ_DOC_URL=$(PRJ_URL)
GIT_USER?=$(USER)
GIT_DESCRIBE_TAG=$(shell git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)
PRJ_PACKAGE:=$(PRJ)
PYTHON_VERSION?=3.9
PYTHON_VERSION_MAX:=3.9
PYTHONWARNINGS=ignore
PYTHON_PARAMS?=

PYTHON_SRC=$(shell find -L "$(PRJ)" -type f -iname '*.py' | grep -v __pycache__)
PYTHON_TST=$(shell find -L tests -type f -iname '*.py' | grep -v __pycache__)

export DATA?=data


# Conda environment
# To optimize conda, use mamba
CONDA?=mamba
export MAMBA_NO_BANNER=1
CONDA_BASE:=$(shell conda info --base)
CONDA_PACKAGE:=$(CONDA_PREFIX)/lib/python$(PYTHON_VERSION)/site-packages
CONDA_PYTHON:=$(CONDA_PREFIX)/bin/python
CONDA_BLD_DIR?=$(CONDA_PREFIX)/conda-bld
CONDA_CHANNELS?=-c rapidsai -c nvidia -c conda-forge
CONDA_ARGS?=
CONDA_RECIPE=conda-recipe/staged-recipes/recipes/virtual_dataframe

export SETUPTOOLS_SCM_PRETEND_VERSION
export VIRTUAL_ENV=$(CONDA_PREFIX)

PIP_PACKAGE:=$(CONDA_PACKAGE)/$(PRJ_PACKAGE).egg-link
PIP_ARGS?=

# List your labextensions separated with space
JUPYTER_LABEXTENSIONS:=dask-labextension

JUPYTER_DATA_DIR:=$(shell jupyter --data-dir 2>/dev/null || echo "~/.local/share/jupyter")
JUPYTER_LABEXTENSIONS_DIR:=$(CONDA_PREFIX)/share/jupyter/labextensions
_JUPYTER_LABEXTENSIONS:=$(foreach ext,$(JUPYTER_LABEXTENSIONS),$(JUPYTER_LABEXTENSIONS_DIR)/$(ext))

# Project variable
export VDF_MODES=pandas cudf dask dask_modin dask_cudf pyspark

CHECK_GIT_STATUS=[[ `git status --porcelain` ]] && echo "$(yellow)Warning: All files are not commited$(normal)"

# ---------------------------------------------------------------------------------------
# SNIPPET pour ajouter des repositories complémentaires à PIP.
# A utiliser avec par exemple
# pip $(EXTRA_INDEX) install ...
EXTRA_INDEX:=

# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer automatiquement l'aide du Makefile.
# Il faut utiliser des commentaires commençant par '##' précédant la ligne des recettes,
# pour une production automatique de l'aide.
.PHONY: help
.DEFAULT: help

## Print all majors target
help:
	@echo "$(bold)Available rules:$(normal)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=20 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')

	echo -e "Use '$(cyan)make -jn ...$(normal)' for Parallel run"
	echo -e "Use '$(cyan)make -B ...$(normal)' to force the target"
	echo -e "Use '$(cyan)make -n ...$(normal)' to simulate the build"

# ---------------------------------------------------------------------------------------
# SNIPPET pour affichier la valeur d'une variable d'environnement
# tel quelle est vue par le Makefile. Par exemple `make dump-CONDA_PACKAGE`
.PHONY: dump-*
dump-%:
	@if [ "${${*}}" = "" ]; then
		echo "Environment variable $* is not set";
		exit 1;
	else
		echo "$*=${${*}}";
	fi

# ---------------------------------------------------------------------------------------
# SNIPPET pour invoker un shell dans le context du Makefile
.PHONY: shell
## Run shell in Makefile context
shell:
	$(SHELL)


# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer les Notebooks avec GIT.
# Les recettes suivantes s'assure que git est bien initialisé
# et ajoute des recettes pour les fichiers *.ipynb
# et eventuellement pour les fichiers *.csv.
#
# Pour cela, un fichier .gitattribute est maintenu à jour.
# Les recettes pour les notebooks se chargent de les nettoyer avant de les commiter.
# Pour cela, elles appliquent `jupyter nb-convert` à la volée. Ainsi, les comparaisons
# de version ne sont plus parasités par les data.
#
# Les scripts pour les CSV utilisent le composant `daff` (pip install daff)
# pour comparer plus efficacement les évolutions des fichiers csv.
# Un `git diff toto.csv` est plus clair.

# S'assure de la présence de git (util en cas de synchronisation sur le cloud par exemple,
# après avoir exclus le répertoire .git (cf. ssh-ec2)
.git:
	@if [[ ! -d .git ]]; then
		@git init -q
		git commit --allow-empty -m "Create project $(PRJ)"
	fi

# Règle technique importante, invoquées lors du `git commit` d'un fichier *.ipynb via
# le paramètrage de `.gitattributes`.
.PHONY: pipe_clear_jupyter_output
pipe_clear_jupyter_output:
	jupyter nb-convert --to notebook --ClearOutputPreprocessor.enabled=True <(cat <&0) --stdout 2>/dev/null

# Règle qit install git lfs si nécessaire
.git/hooks/post-checkout:
ifeq ($(shell which git-lfs >/dev/null ; echo "$$?"),0)
	# Add git lfs if possible
	@git lfs install >/dev/null
	# Add some extensions in lfs
	@git lfs track "*.pkl" --lockable  >/dev/null
	@git lfs track "*.bin" --lockable  >/dev/null
	@git lfs track "*.jpg" --lockable  >/dev/null
	@git lfs track "*.jpeg" --lockable >/dev/null
	@git lfs track "*.gif" --lockable  >/dev/null
	@git lfs track "*.png" --lockable  >/dev/null
endif

# Règle qui ajoute la validation du project avant un push sur la branche master.
# Elle ajoute un hook git pour invoquer `make validate` avant de pusher. En cas
# d'erreur, le push n'est pas possible.
# Pour forcer le push, malgres des erreurs lors de l'exécution de 'make validate'
# utilisez 'FORCE=y git push'.
# Pour supprimer ce comportement, il faut modifier le fichier .git/hooks/pre-push
# et supprimer la règle du Makefile, ou bien,
# utiliser un fichier vide 'echo ''> .git/hooks/pre-push'
.git/hooks/pre-push:.git/hooks/post-checkout | .git
	@# Add a hook to validate the project before a git push
	cat >>.git/hooks/pre-push <<PRE-PUSH
	#!/usr/bin/env sh
	# Validate the project before a push
	if test -t 1; then
		ncolors=$$(tput colors)
		if test -n "\$$ncolors" && test \$$ncolors -ge 8; then
			normal="\$$(tput sgr0)"
			red="\$$(tput setaf 1)"
	        green="\$$(tput setaf 2)"
			yellow="\$$(tput setaf 3)"
		fi
	fi
	branch="\$$(git branch | grep \* | cut -d ' ' -f2)"
	if [ "\$${branch}" = "master" ] && [ "\$${FORCE}" != y ] ; then
		printf "\$${green}Validate the project before push the commit... (\$${yellow}make validate\$${green})\$${normal}\n"
		make validate
		ERR=\$$?
		if [ \$${ERR} -ne 0 ] ; then
			printf "\$${red}'\$${yellow}make validate\$${red}' failed before git push.\$${normal}\n"
			printf "Use \$${yellow}FORCE=y git push\$${normal} to force.\n"
			exit \$${ERR}
		fi
	fi
	PRE-PUSH
	chmod +x .git/hooks/pre-push

# Init git configuration

.gitattributes: | .git .git/hooks/pre-push  # Configure git
	@git config --local core.autocrlf input
	# Set tabulation to 4 when use 'git diff'
	@git config --local core.page 'less -x4'

ifeq ($(shell which jupyter >/dev/null ; echo "$$?"),0)
	# Add rules to manage the output data of notebooks
	@git config --local filter.dropoutput_jupyter.clean "make --silent pipe_clear_jupyter_output"
	@git config --local filter.dropoutput_jupyter.smudge cat
	@[ -e .gitattributes ] && grep -v dropoutput_jupyter .gitattributes >.gitattributes.new 2>/dev/null || true
	@[ -e .gitattributes.new ] && mv .gitattributes.new .gitattributes || true
	@echo "*.ipynb filter=dropoutput_jupyter diff=dropoutput_jupyter -text" >>.gitattributes
endif

ifeq ($(shell which daff >/dev/null ; echo "$$?"),0)
	# Add rules to manage diff with daff for CSV file
	@git config --local diff.daff-csv.command "daff.py diff --git"
	@git config --local merge.daff-csv.name "daff.py tabular merge"
	@git config --local merge.daff-csv.driver "daff.py merge --output %A %O %A %B"
	@[ -e .gitattributes ] && grep -v daff-csv .gitattributes >.gitattributes.new 2>/dev/null
	@[ -e .gitattributes.new ] && mv .gitattributes.new .gitattributes
	@echo "*.[tc]sv diff=daff-csv merge=daff-csv -text" >>.gitattributes
endif


# ---------------------------------------------------------------------------------------
# SNIPPET pour vérifier la présence d'un environnement Conda conforme
# avant le lancement d'un traitement.
# Il faut ajouter $(VALIDATE_VENV) dans les recettes
# et choisir la version à appliquer.
# Soit :
# - CHECK_VENV pour vérifier l'activation d'un VENV avant de commencer
# - ACTIVATE_VENV pour activer le VENV avant le traitement
# Pour cela, sélectionnez la version de VALIDATE_VENV qui vous convient.
# Attention, toute les règles proposées ne sont pas compatible avec le mode ACTIVATE_VENV
CHECK_VENV=if [[ "$(VENV)" != "$(CONDA_DEFAULT_ENV)" ]] ; \
  then echo -e "$(green)Use: $(cyan)conda activate $(VENV)$(green) before using 'make'$(normal)"; exit 1 ; fi

ACTIVATE_VENV=source $(CONDA_BASE)/etc/profile.d/conda.sh && conda activate $(VENV) $(CONDA_ARGS) $(CONDA_CHANNELS)
DEACTIVATE_VENV=source $(CONDA_BASE)/etc/profile.d/conda.sh && conda deactivate

VALIDATE_VENV=$(CHECK_VENV)
#VALIDATE_VENV=$(ACTIVATE_VENV)

# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer correctement toute les dépendences python du projet.
# La cible `requirements` se charge de gérer toutes les dépendences
# d'un projet Python. Dans le SNIPPET présenté, il y a de quoi gérer :
# - les dépendances PIP
# - la gestion d'un kernel pour Jupyter
#
# Il suffit, dans les autres de règles, d'ajouter la dépendances sur `$(REQUIREMENTS)`
# pour qu'un simple `make test` garantie la mise à jour de l'environnement avant
# le lancement des tests par exemple.
#
# Pour cela, il faut indiquer dans le fichier 'setup.py', toutes les dépendances
# de run et de test (voir le modèle de `setup.py` proposé)

# All dependencies of the project must be here
$(CONDA_PACKAGE): environment-gpu.yml environment-dev.yml
	@$(VALIDATE_VENV)
ifeq ($(USE_GPU),-gpu)
	echo "$(green)  Install conda dependencies...$(normal)"
	$(CONDA) env update \
		-q $(CONDA_ARGS) \
		--file environment-gpu.yml
endif
	$(CONDA) env update \
		-q $(CONDA_ARGS) \
		--file environment-dev.yml
	touch $(CONDA_PACKAGE)


.PHONY: requirements dependencies
REQUIREMENTS= $(CONDA_PACKAGE) $(PIP_PACKAGE) \
	.gitattributes
requirements: $(REQUIREMENTS)
dependencies: requirements


# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer le mode offline.
# La cible `offline` permet de télécharger toutes les dépendences, pour pouvoir les utiliser
# ensuite sans connexion. Ensuite, il faut valoriser la variable d'environnement OFFLINE
# à True avant le lancement du make pour une utilisation sans réseau.
# `export OFFLINE=True
# make ...
# unset OFFLINE`

# Download dependencies for offline usage
~/.mypypi: setup.py
	@pip download '.' --dest ~/.mypypi
# Download modules and packages before going offline
offline: ~/.mypypi
ifeq ($(OFFLINE),True)
CONDA_ARGS+=--use-index-cache --offline
PIP_ARGS+=--no-index --find-links ~/.mypypi
endif

# Rule to check the good installation of python in Conda venv
$(CONDA_PYTHON):
	@$(VALIDATE_VENV)
	$(CONDA) install -q "python=$(PYTHON_VERSION).*" -y $(CONDA_ARGS) $(CONDA_CHANNELS)

# Rule to update the current venv, with the dependencies describe in `setup.py`
$(PIP_PACKAGE): $(CONDA_PYTHON) setup.py | .git # Install pip dependencies
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Install setup.py dependencies ... (may take minutes)$(normal)"
	pip install $(PIP_ARGS) $(EXTRA_INDEX) -e '.' | grep -v 'already satisfied' || true
	echo -e "$(cyan)setup.py dependencies updated$(normal)"
	@touch $(PIP_PACKAGE)

# ---------------------------------------------------------------------------------------
# SNIPPET pour gérer les kernels Jupyter
# Intall a Jupyter kernel
$(JUPYTER_DATA_DIR)/kernels/$(KERNEL): $(REQUIREMENTS)
	@$(VALIDATE_VENV)
	python $(PYTHON_ARGS) -O -m ipykernel install --user --name $(KERNEL)
	echo -e "$(cyan)Kernel $(KERNEL) installed$(normal)"

# Remove the Jupyter kernel
.PHONY: remove-kernel
remove-kernel: $(REQUIREMENTS)
	@echo y | jupyter kernelspec uninstall $(KERNEL) 2>/dev/null || true
	echo -e "$(yellow)Warning: Kernel $(KERNEL) uninstalled$(normal)"


# ---------------------------------------------------------------------------------------
# SNIPPET pour gener les extensions jupyter
#$(JUPYTER_LABEXTENSIONS)/dask-labextension:
#	jupyter labextension install dask-labextension
$(JUPYTER_LABEXTENSIONS_DIR)/%:
	@echo -e "$(green)Install jupyter labextension $* $(normal)"
	jupyter labextension install $*

# ---------------------------------------------------------------------------------------
# SNIPPET pour préparer l'environnement d'un projet juste après un `git clone`
.PHONY: configure
$(CONDA_HOME)/bin/mamba:
	@echo -e "$(green)Install mamba$(normal)"
	conda install mamba -n base -c conda-forge -y

## Prepare the work environment (conda venv, kernel, ...)
configure: $(CONDA_HOME)/bin/mamba
	@if [[ "$(CONDA_DEFAULT_ENV)" != "base" ]] ; \
      then echo -e "$(green)Use: $(cyan)conda deactivate $(VENV)$(green) before using 'make'$(normal)"; exit 1 ; fi
	$(CONDA) create \
		--name "$(VENV)" \
		-y $(CONDA_ARGS) \
		python==$(PYTHON_VERSION)
	touch environment-*.yml
	@if [[ "base" == "$(CONDA_DEFAULT_ENV)" ]] || [[ -z "$(CONDA_DEFAULT_ENV)" ]] ; \
	then echo -e "Use: $(cyan)conda activate $(VENV)$(normal)" ; fi

# ---------------------------------------------------------------------------------------
.PHONY: remove-venv
remove-$(VENV):
	@$(DEACTIVATE_VENV)
	$(CONDA) env remove --name "$(VENV)" -y 2>/dev/null
	echo -e "Use: $(cyan)conda deactivate$(normal)"
# Remove virtual environement
remove-venv : remove-$(VENV)

# ---------------------------------------------------------------------------------------
# SNIPPET de mise à jour des dernières versions des composants.
# Après validation, il est nécessaire de modifier les versions dans le fichier `setup.py`
# pour tenir compte des mises à jours
.PHONY: upgrade-venv
upgrade-$(VENV):
ifeq ($(OFFLINE),True)
	@echo -e "$(red)Can not upgrade virtual env in offline mode$(normal)"
else
	@$(VALIDATE_VENV)
	$(CONDA) update --all $(CONDA_ARGS) $(CONDA_CHANNELS)
	pip list --format freeze --outdated | sed 's/(.*//g' | xargs -r -n1 pip install $(EXTRA_INDEX) -U
	@echo -e "$(cyan)After validation, upgrade the setup.py$(normal)"
endif
# Upgrade packages to last versions
upgrade-venv: upgrade-$(VENV)

# ---------------------------------------------------------------------------------------
# SNIPPET de validation des notebooks en les ré-executants.
# L'idée est d'avoir un sous répertoire par phase, dans le répertoire `notebooks`.
# Ainsi, il suffit d'un `make nb-run-phase1` pour valider tous les notesbooks du répertoire `notebooks/phase1`.
# Pour valider toutes les phases : `make nb-run-*`.
# L'ordre alphabétique est utilisé. Il est conseillé de préfixer chaque notebook d'un numéro.
.PHONY: nb-run-*
notebooks/.make-%: $(REQUIREMENTS) $(JUPYTER_DATA_DIR)/kernels/$(KERNEL)
	$(VALIDATE_VENV)
	time jupyter nbconvert \
	  --ExecutePreprocessor.timeout=-1 \
	  --execute \
	  --inplace notebooks/$*/*.ipynb
	date >notebooks/.make-$*
# All notebooks
notebooks/phases: $(sort $(subst notebooks/,notebooks/.make-,$(wildcard notebooks/*)))

## Invoke all notebooks in lexical order from notebooks/<% dir>
nb-run-%: $(JUPYTER_DATA_DIR)/kernels/$(KERNEL)
	@VENV=$(VENV) $(MAKE) --no-print-directory notebooks/.make-$*


# ---------------------------------------------------------------------------------------
# SNIPPET de validation des scripts en les ré-executant.
# Ces scripts peuvent être la traduction de Notebook Jupyter, via la règle `make nb-convert`.
# L'idée est d'avoir un sous répertoire par phase, dans le répertoire `scripts`.
# Ainsi, il suffit d'un `make run-phase1` pour valider tous les scripts du répertoire `scripts/phase1`.
# Pour valider toutes les phases : `make run-*`.
# L'ordre alphabétique est utilisé. Il est conseillé de préfixer chaque script d'un numéro.
.PHONY: run-*
scripts/.make-%: $(REQUIREMENTS)
	@$(VALIDATE_VENV)
	time ls scripts/$*/*.py | grep -v __ | sed 's/\.py//g; s/\//\./g' | \
		xargs -L 1 -t python -O -m
	@date >scripts/.make-$*

# All phases
scripts/phases: $(sort $(subst scripts/,scripts/.make-,$(wildcard scripts/*)))

## Invoke all script in lexical order from scripts/<% dir>
run-%:
	@$(MAKE) --no-print-directory scripts/.make-$*

# ---------------------------------------------------------------------------------------
# SNIPPET pour valider le code avec flake8 et pylint
.PHONY: lint
.pylintrc:
	@pylint --generate-rcfile > .pylintrc

.make-lint: $(REQUIREMENTS) $(PYTHON_SRC) | .pylintrc
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Check lint...$(normal)"
	echo "---------------------- FLAKE"
	flake8 $(PRJ_PACKAGE)
	touch .make-lint

## Lint the code
lint: .make-lint


# ---------------------------------------------------------------------------------------
# SNIPPET pour valider le typage avec pytype
$(CONDA_PREFIX)/bin/pytype:
	@pip install $(PIP_ARGS) -q pytype

pytype.cfg: $(CONDA_PREFIX)/bin/pytype
	@[[ ! -f pytype.cfg ]] && pytype --generate-config pytype.cfg || true
	touch pytype.cfg

.PHONY: typing
.make-typing: $(REQUIREMENTS) $(CONDA_PREFIX)/bin/pytype pytype.cfg $(PYTHON_SRC)
ifneq ($(USE_GPU),-gpu)
	@echo -e "$(red)Ignore typing without GPU$(normal)"
else
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Check typing...$(normal)"
	# pytype
	pytype --config=pytype.cfg "$(PRJ)"
	for phase in scripts/*
	do
	  [[ -e "$$phase" ]] && ( cd $$phase && find -L . -type f -name '*.py' -exec pytype --config=pytype.cfg {} \; )
	done
	touch ".pytype/pyi/$(PRJ)"
endif
	# mypy
	# MYPYPATH=./stubs/ mypy "$(PRJ)"
	touch .make-typing

## Check python typing
typing: .make-typing

## Add infered typing in module
add-typing: typing
	@find -L "$(PRJ)" -type f -name '*.py' -exec merge-pyi -i {} .pytype/pyi/{}i \;
	for phase in scripts/*
	do
	  ( cd $$phase ; find -L . -type f -name '*.py' -exec merge-pyi -i {} .pytype/pyi/{}i \; )
	done







# ---------------------------------------------------------------------------------------
# SNIPPET pour créer la documentation html à partir de fichier Markdown.
.PHONY: docs docs-serve docs-gh-deploy
## Generate and show the HTML documentation
docs:
	@$(VALIDATE_VENV)
	mkdocs build
	$(BROWSER) site/index.html

## Start an html server with the current documentation
docs-serve:
	@$(VALIDATE_VENV)
	mkdocs serve

## Deploy the document to github static pages
docs-gh-deploy:
	@$(VALIDATE_VENV)
	mkdocs gh-deploy

# ---------------------------------------------------------------------------------------
# SNIPPET pour créer une distribution des sources
.PHONY: sdist
dist/$(PRJ_PACKAGE)-*.tar.gz: $(REQUIREMENTS)
	@$(VALIDATE_VENV)
	python $(PYTHON_ARGS) setup.py sdist

# Create a source distribution
sdist: dist/$(PRJ_PACKAGE)-*.tar.gz

# ---------------------------------------------------------------------------------------
# SNIPPET pour créer une distribution des binaires au format egg.
# Pour vérifier la version produite :
# python setup.py --version
# Cela correspond au dernier tag d'un format 'version'
.PHONY: bdist
dist/$(subst -,_,$(PRJ_PACKAGE))-*.whl: $(REQUIREMENTS) $(PYTHON_SRC)
	@$(VALIDATE_VENV)
	python $(PYTHON_ARGS) setup.py bdist_wheel

# Create a binary wheel distribution
bdist: dist/$(subst -,_,$(PRJ_PACKAGE))-*.whl

# ---------------------------------------------------------------------------------------
# SNIPPET pour créer une distribution des binaires au format conda.
.PHONY:conda-build conda-install conda-debug conda-convert conda-create
export PRJ
export REMOTE_GIT_URL
export PRJ_URL
export PRJ_DOC_URL
export GIT_DESCRIBE_TAG
export PRJ_PACKAGE
export PYTHON_VERSION
export PYTHON_VERSION_MAX

CONDA_USER?=${USER}
CONDA_TOKEN?=""
CONDA_BUILD_TARGET=${CONDA_BLD_DIR}/noarch/${PRJ}-*.tar.bz2

$(CONDA_PREFIX)/bin/conda-build:
	@$(CONDA) install conda-build conda-verify -y

$(CONDA_BLD_DIR):
	@mkdir -p $(CONDA_BLD_DIR)
	$(CONDA) index $(CONDA_BLD_DIR)

DEBUG_CONDA=--dirty #--keep-old-work --debug --no-remove-work-dir --no-long-test-prefix --no-build-id
${CONDA_BUILD_TARGET}: $(REQUIREMENTS) $(CONDA_PACKAGE) clean-build conda-purge dist/$(subst -,_,$(PRJ_PACKAGE))-*.whl $(CONDA_BLD_DIR) \
		$(CONDA_PREFIX)/bin/conda-build \
		conda-recipe/meta.template.yaml _rm_meta setup.* \
		$(CONDA_RECIPE)/meta.yaml
	@$(VALIDATE_VENV)
	$(CHECK_GIT_STATUS)
	# cp -f --reflink=auto dist/*.whl conda-recipe/
	export GIT_DESCRIBE_TAG=$(shell python setup.py --version 2>/dev/null)
	export WHEEL=$(subst -,_,$(PRJ_PACKAGE))-*.whl

	# Note: due to a bug in conda-build, it's impossible to run the test with
	# app packages at the time. So, I desactivate the tests now

	$(CONDA) mambabuild \
		$(CONDA_CHANNELS) \
		${CONDA_ARGS} \
		${DEBUG_CONDA} \
		--output-folder ${CONDA_BLD_DIR} \
		--python=$(PYTHON_VERSION) \
		--no-anaconda-upload \
		$(CONDA_RECIPE)
	cp --reflink=auto \
		${CONDA_BLD_DIR}/noarch/${PRJ}*.tar.bz2 \
		dist/
	echo -e "$(green)Conda package builded in ${CONDA_BLD_DIR}/noarch/${PRJ}*-$${GIT_DESCRIBE_TAG}*.tar.bz2$(normal)"
	touch ${CONDA_BUILD_TARGET}

# Remove alternative environment
conda-remove-envs:
	@$(VALIDATE_VENV)
	for mode in $(VDF_MODES)
	do
		$(CONDA) env remove -n test-$$mode
	done

## Build the conda packages
conda-build: $(REQUIREMENTS) $(CONDA_PREFIX)/bin/conda-build ${CONDA_BUILD_TARGET}

# Check conda recipe
conda-check: $(REQUIREMENTS) $(CONDA_RECIPE)/meta.yaml
	@$(VALIDATE_VENV)
	conda smithy recipe-lint $(CONDA_RECIPE)

## Purge the conda build process
conda-purge: $(REQUIREMENTS) conda-remove-envs
	@$(VALIDATE_VENV)
	$(CONDA) build purge \
		--output-folder ${CONDA_BLD_DIR}
	rm -f conda-recipe/*.whl
	echo -e "$(cyan)Conda cleaned$(normal)"


## Debug the conda build process (make OUTPUT_ID="*-$VDF_MODE" conda-debug)
conda-debug: $(REQUIREMENTS) $(CONDA_RECIPE)/meta.yaml
	@$(VALIDATE_VENV)
	$(CHECK_GIT_STATUS)
	# cp -f --reflink=auto dist/*.whl conda-recipe/
	export GIT_DESCRIBE_TAG=$(shell python setup.py --version 2>/dev/null)
	export WHEEL=$(subst -,_,$(PRJ_PACKAGE))-*.whl
	$(CONDA) debug \
		$(CONDA_CHANNELS) \
		${CONDA_ARGS} \
		--output-id="$(OUTPUT_ID)" \
		$(CONDA_RECIPE)

## Convert the package for all platform
conda-convert: ${CONDA_BUILD_TARGET} $(CONDA_RECIPE)/meta.yaml
	@$(CONDA) convert \
		--platform all \
		-o dist/ \
		${CONDA_BUILD_TARGET}

## Install the built conda package
conda-install: ${CONDA_BUILD_TARGET} $(CONDA_RECIPE)/meta.yaml
	@$(VALIDATE_VENV)
	$(CONDA) install \
		-c ${CONDA_BLD_DIR} \
		$(CONDA_CHANNELS) \
		$(PRJ_PACKAGE)

## Install a specific version of conda package
conda-install-%: ${CONDA_BUILD_TARGET} $(CONDA_RECIPE)/meta.yaml
	@$(CHECK_GIT_STATUS)
	$(CONDA) install \
		-c file://${PWD}/${CONDA_BLD_DIR} $(CONDA_CHANNELS) ${CONDA_ARGS} \
		-y \
		${PRJ}-$*

## Create all tests environments
conda-create: $(CONDA_RECIPE)/meta.yaml # ${CONDA_BUILD_TARGET}
	@$(VALIDATE_VENV)
	for mode in $(VDF_MODES)
	do
		$(CONDA) create -y -n test-$$mode $(CONDA_ARGS) \
		  -c file://${PWD}/${CONDA_BLD_DIR} $(CONDA_CHANNELS)\
		  virtual_dataframe-$$mode
		CONDA_PREFIX=$(CONDA_HOME)/envs/test-$$mode \
		$(CONDA) env config vars set VDF_MODE=$$mode
	done

# ---------------------------------------------------------------------------------------
# SNIPPET pour créer publier une version dans conda-forge.
# La procedure n'est pas simple. Il faut commencer par avoir un clone
# du projet staged-recipes. (Voir https://conda-forge.org/docs/maintainer/adding_pkgs.html)
# A partir de celui-ci, et d'un tag déposé pour la version,
# et une publication de release dans github
# le code se charge de publier la version.

.PHONY:test-conda-forge conda-forge _rm_meta _prepare-conda-forge

_rm_meta:
	rm -f $(CONDA_RECIPE)/meta.yaml

conda-recipe/staged-recipes:
	@$(VALIDATE_VENV)
	# Create a user fork of staged-recipes
	gh repo fork --remote https://github.com/conda-forge/staged-recipes conda-recipe/staged-recipes
	(
		cd conda-recipe/staged-recipes ; \
		git checkout -b $(PRJ_PACKAGE) origin/$(PRJ_PACKAGE)\
	)
	GIT_USER=$$(gh auth status -t 2>&1 | grep oauth_token | awk '{ print $$7 }')
	#git submodule add --force https://github.com/$$GIT_USER/staged-recipes.git conda-recipe/staged-recipes

conda-recipe/staged-recipes/recipes/$(PRJ_PACKAGE): conda-recipe/staged-recipes
	@mkdir conda-recipe/staged-recipes/recipes/$(PRJ_PACKAGE)/$(PRJ_PACKAGE).tar.gz
	echo -e "$(yellow)See the directory $(CONDA_RECIPE)$(normal)"

$(CONDA_RECIPE)/meta.yaml: $(CONDA_RECIPE) conda-recipe/meta.template.yaml
	@$(VALIDATE_VENV)
	PRJ_VERSION=v$(shell python setup.py --version)
	URL="https://github.com/pprados/$(PRJ_PACKAGE)/tarball/$$PRJ_VERSION/$(PRJ_PACKAGE).tar.gz"
	HASH=$$(wget -q "$$URL" -O - | sha256sum | awk '{print $$1}')
	# Inject HASH and version
	sed "s/<<VERSION>>/$$PRJ_VERSION/g; s/<<HASH>>/$$HASH/g;" \
		<conda-recipe/meta.template.yaml \
		>$(CONDA_RECIPE)/meta.yaml
	( \
		cd $(CONDA_RECIPE) ; \
		conda smithy recipe-lint . && \
		git add meta.yaml \
	)
	echo -e "$(green)Inject version $$PRJ_VERSION$(normal)"

# Test a new version of meta.yaml files
test-conda-forge: _rm_meta $(CONDA_RECIPE)/meta.yaml
	@(
		cd $(CONDA_RECIPE) ;
		git commit -a --amend -m "Test $(PRJ_PACKAGE)" $$RECIPE ;
		git push --force
	)
	echo -e "$(green)A new version was published. The CI will begin to test \
	(https://github.com/conda-forge/staged-recipes/pulls?q=$(PRJ_PACKAGE)$(normal)"

# See https://conda-forge.org/docs/maintainer/adding_pkgs.html
## First release in conda-forge. Add a version tag before use.
conda-forge: _rm_meta $(CONDA_RECIPE)/meta.yaml
	@PRJ_VERSION=v$(shell python setup.py --version)
	@(
		cd $(CONDA_RECIPE) ;
		git commit -a -m "Release $(PRJ_VERSION)" $$RECIPE ;
		git push
	)

# ---------------------------------------------------------------------------------------
# SNIPPET pour créer une distribution des binaires au format whl.
# Pour vérifier la version produite :
# python setup.py --version
# Cela correspond au dernier tag d'un format 'version'
.PHONY: dist

## Create a full distribution
dist: bdist sdist

# ---------------------------------------------------------------------------------------
# SNIPPET pour tester la publication d'une distribution avant sa publication.
.PHONY: check-twine
## Check the distribution before publication
check-twine: bdist
ifeq ($(OFFLINE),True)
	@echo -e "$(red)Can not check-twine in offline mode$(normal)"
else
	@$(VALIDATE_VENV)
	twine check \
		$(shell find dist -type f \( -name "*.whl" -or -name '*.gz' \) -and ! -iname "*dev*" )
endif

# ---------------------------------------------------------------------------------------
# SNIPPET pour tester la publication d'une distribution
# sur test.pypi.org.
.PHONY: test-twine
## Publish distribution on test.pypi.org
test-twine: bdist
ifeq ($(OFFLINE),True)
	@echo -e "$(red)Can not test-twine in offline mode$(normal)"
else
	@$(VALIDATE_VENV)
	rm -f dist/*.asc
	twine upload --sign --repository-url https://test.pypi.org/legacy/ \
		$(shell find dist -type f \( -name "*.whl" -or -name '*.gz' \) -and ! -iname "*dev*" )
endif

# ---------------------------------------------------------------------------------------
# SNIPPET pour publier la version sur pypi.org.
.PHONY: release
## Publish distribution on pypi.org
release: clean dist
ifeq ($(OFFLINE),True)
	@echo -e "$(red)Can not release in offline mode$(normal)"
else
	@$(VALIDATE_VENV)
	[[ $$( find dist -name "*.dev*" | wc -l ) == 0 ]] || \
		( echo -e "$(red)Add a tag version in GIT before release$(normal)" \
		; exit 1 )
	rm -f dist/*.asc
	echo "Enter Pypi password"
	twine upload --sign \
		$(shell find dist -type f \( -name "*.whl" -or -name '*.gz' \) -and ! -iname "*dev*" )

endif



# ---------------------------------------------------------------------------------------
# SNIPPET pour executer jupyter notebook, mais en s'assurant de la bonne application des dépendances.
# Utilisez `make notebook` à la place de `jupyter notebook`.
.PHONY: notebook
## Start jupyter notebooks
notebook: $(REQUIREMENTS) $(JUPYTER_DATA_DIR)/kernels/$(KERNEL) $(_JUPYTER_LABEXTENSIONS)
	@$(VALIDATE_VENV)
	DATA=$$DATA jupyter lab \
		--notebook-dir=notebooks \
		--ExecutePreprocessor.kernel_name=$(KERNEL)


# Download raw data if necessary
$(DATA)/raw:



# ---------------------------------------------------------------------------------------
# SNIPPET pour convertir tous les notebooks de 'notebooks/' en script
# python dans 'scripts/', déjà compatible avec le mode scientifique de PyCharm Pro.
# Le code utilise un modèle permettant d'encadrer les cellules Markdown dans des strings.
# Les scripts possèdent ensuite le flag d'exécution, pour pouvoir les lancer directement
# via un 'scripts/phase1/1_sample.py'.
.PHONY: nb-convert
# Convert all notebooks to python scripts
_nbconvert:  $(JUPYTER_DATA_DIR)/kernels/$(KERNEL)
	@echo -e "Convert all notebooks..."
	notebook_path=notebooks
	script_path=scripts
	tmpdir=$$(mktemp -d -t make-XXXXX)
	tmpfile=$${tmpdir}/template
	cat >$${tmpfile} <<TEMPLATE
	{% extends 'python.tpl' %}
	{% block in_prompt %}# %%{% endblock in_prompt %}
	{%- block header -%}
	#!/usr/bin/env python
	# coding: utf-8
	{% endblock header %}
	{% block input %}
	{{ cell.source | ipython2python }}{% endblock input %}
	{% block markdowncell scoped %}
	# %% md
	"""
	{{ cell.source  }}
	"""
	{% endblock markdowncell %}
	TEMPLATE

	# --template=$${tmpfile} \
	while IFS= read -r -d '' filename; do
		target=$$(echo $$filename | sed "s/^$${notebook_path}/$${script_path}/g; s/ipynb$$/py/g ; s/[ -]/_/g" )
		mkdir -p $$(dirname $${target})
		jupyter nbconvert --to python --ExecutePreprocessor.kernel_name=$(KERNEL) \
          --TemplateExporter.extra_template_basedirs=$${tmpdir} \
		  --stdout "$${filename}" >"$${target}"
		chmod +x $${target}
		@echo -e "Convert $${filename} to $${target}"
	done < <(find -L notebooks -name '*.ipynb' -type f -not -path '*/\.*' -prune -print0)
	echo -e "$(cyan)All new scripts are in $${target}$(normal)"

# Version permettant de convertir les notebooks et de la ajouter en même temps à GIT
# en ajoutant le flag +x.
## Convert all notebooks to python scripts
nb-convert: _nbconvert
	@find -L scripts/ -type f -iname "*.py" -exec git add "{}" \;
	find -L scripts/ -type f -iname "*.py" -exec git update-index --chmod=+x  "{}" \;

# ---------------------------------------------------------------------------------------
# SNIPPET pour nettoyer tous les fichiers générés par le compilateur Python.
.PHONY: clean-pyc
# Clean pre-compiled files
clean-pyc:
	@/usr/bin/find -L . -type f -name "*.py[co]" -delete
	/usr/bin/find -L . -type d -name "__pycache__" -delete

# ---------------------------------------------------------------------------------------
# SNIPPET pour nettoyer les fichiers de builds (package et docs).
.PHONY: clean-build
# Remove build artifacts and docs
clean-build:
	@/usr/bin/find -L . -type f -name ".make-*" -delete
	rm -fr build/ dist/* *.egg-info .eggs .repository conda-recipe/*.whl
	echo -e "$(cyan)Build cleaned$(normal)"

# ---------------------------------------------------------------------------------------
# SNIPPET pour nettoyer tous les notebooks
.PHONY: clean-notebooks
## Remove all results in notebooks
clean-notebooks: $(REQUIREMENTS)
	@[ -e notebooks ] && find -L notebooks -name '*.ipynb' -exec jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace {} \;
	echo -e "$(cyan)Notebooks cleaned$(normal)"

# ---------------------------------------------------------------------------------------
.PHONY: clean-pip
# Remove all the pip package
clean-pip:
	@$(VALIDATE_VENV)
	pip freeze | grep -v -e "^-e" -e " @.*" -e "^#.*" | xargs pip uninstall -y
	echo -e "$(cyan)Virtual env cleaned$(normal)"

# ---------------------------------------------------------------------------------------
# SNIPPET pour nettoyer complètement l'environnement Conda
.PHONY: clean-venv clean-$(VENV)
clean-$(VENV): remove-venv
	@$(CONDA) create -y -q -n $(VENV) $(CONDA_ARGS) $(CONDA_CHANNELS)
	touch setup.py
	echo -e "$(yellow)Warning: Conda virtualenv $(VENV) is empty.$(normal)"
# Set the current VENV empty
clean-venv : clean-$(VENV)

# ---------------------------------------------------------------------------------------
# SNIPPET pour faire le ménage du projet (hors environnement)
.PHONY: clean
## Clean current environment
clean: clean-pyc clean-build clean-notebooks conda-purge
	@rm -Rf dask-worker-space spark-warehouse site

# ---------------------------------------------------------------------------------------
# SNIPPET pour faire le ménage du projet
.PHONY: clean-all
# Clean all environments
clean-all: remove-kernel clean remove-venv
	@rm -Rf .pytest_cache .pytype .pytest_cache dask-worker-space/

# ---------------------------------------------------------------------------------------
# SNIPPET pour executer les tests unitaires et les tests fonctionnels.
# Utilisez 'NPROC=1 make unit-test' pour ne pas paralléliser les tests
# Voir https://setuptools.readthedocs.io/en/latest/setuptools.html#test-build-package-and-run-a-unittest-suite
ifeq ($(shell test $(NPROC) -gt 1; echo $$?),0)
PYTEST_ARGS ?=-n $(NPROC)  --dist loadgroup
else
PYTEST_ARGS ?=
endif
.PHONY: test unittest functionaltest
.make-_unit-test-%: $(REQUIREMENTS) $(PYTHON_TST) $(PYTHON_SRC)
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Run unit tests...$(normal)"
	unset DASK_SCHEDULER_SERVICE_PORT
	unset DASK_SCHEDULER_SERVICE_HOST
	python $(PYTHON_ARGS)  -m pytest --rootdir=. -s tests $(PYTEST_ARGS) -m "not functional"
	date >.make-_unit-test-$*

# Run unit test with a specific *mode*
.PHONY: unit-test-*
unit-test-%: $(REQUIREMENTS)
	@echo -e "$(cyan)set VDF_MODE=$*$(normal)"
	VDF_MODE=$* $(MAKE) --no-print-directory .make-_unit-test-$*

ifneq ($(USE_GPU),-gpu)
unit-test-cudf:
	@echo -e "$(red)Ignore notebook with VDF_MODE=cudf$(normal)"

unit-test-dask_cudf:
	@echo -e "$(red)Ignore notebook with VDF_MODE=dask_cudf$(normal)"
endif

.PHONY: unit-test
.make-unit-test: $(foreach ext,$(VDF_MODES),unit-test-$(ext))
	@date >.make-unit-test

## Run unit test for all *mode*
unit-test: .make-unit-test


.PHONY: notebooks-test
.make-notebooks-test-%: $(REQUIREMENTS) $(PYTHON_TST) $(PYTHON_SRC) $(JUPYTER_DATA_DIR)/kernels/$(KERNEL) \
	# notebooks/demo.ipynb
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Run notebook tests for VDF_MODE=$*...$(normal)"
	unset DASK_SCHEDULER_SERVICE_PORT
	unset DASK_SCHEDULER_SERVICE_HOST
	python $(PYTHON_ARGS) -m papermill \
		-k $(KERNEL) \
		--log-level ERROR \
		--no-report-mode \
		-p mode $* \
		notebooks/demo.ipynb \
		/dev/null 2>&1 | grep -v -e "the file is not specified with any extension" -e " *warnings.warn("
	date >.make-notebooks-test-$*

.PHONY: notebooks-test-*
## Run notebooks test with a specific *mode*
notebooks-test-%: $(REQUIREMENTS)
	@$(MAKE) --no-print-directory .make-notebooks-test-$*

ifneq ($(USE_GPU),-gpu)
.make-notebooks-test-cudf:
	@echo -e "$(red)Ignore VDF_MODE=cudf$(normal)"

.make-notebooks-test-dask_cudf:
	@echo -e "$(red)Ignore VDF_MODE=dask_cudf$(normal)"
endif

.PHONY: notebooks-test-all
.make-notebooks-test: $(foreach ext,$(VDF_MODES),.make-notebooks-test-$(ext))
	@date >.make-notebooks-test

## Run notebook test for all *mode*
notebooks-test: .make-notebooks-test

.make-functional-test: $(REQUIREMENTS) $(PYTHON_TST) $(PYTHON_SRC)
	@$(VALIDATE_VENV)
	echo -e "$(cyan)Run functional tests...$(normal)"
	unset DASK_SCHEDULER_SERVICE_PORT
	unset DASK_SCHEDULER_SERVICE_HOST
	python $(PYTHON_ARGS)  -m pytest --rootdir=. -s tests $(PYTEST_ARGS) -m "functional"
	date >.make-functional-test
# Run only functional tests
functional-test: .make-functional-test

.make-test-%: $(REQUIREMENTS) $(PYTHON_TST) $(PYTHON_SRC)
	@echo -e "$(cyan)Run all tests for $(VDF_MODE)...$(normal)"
	unset DASK_SCHEDULER_SERVICE_PORT
	unset DASK_SCHEDULER_SERVICE_HOST
	python $(PYTHON_ARGS)  -m pytest --rootdir=. $(PYTEST_ARGS) -s tests
	#python setup.py test
	date >.make-test-$(VDF_MODE)
	date >.make-unit-test
	date >.make-functional-test

.PHONY: test-*
## Run all tests for a specific *mode* (make test-cudf)
test-%: $(REQUIREMENTS)
	@VDF_MODE=$* $(MAKE) --no-print-directory .make-test-$*

ifneq ($(USE_GPU),-gpu)
test-cudf:
	@echo -e "$(red)Ignore VDF_MODE=cudf$(normal)"

test-dask_cudf:
	@echo -e "$(red)Ignore VDF_MODE=dask_cudf$(normal)"
endif


.make-test: $(foreach ext,$(VDF_MODES),test-$(ext)) .make-notebooks-test
	@date >.make-test

.PHONY: test
## Run all tests (unit and functional) for all *mode*
test: .make-test


# SNIPPET pour vérifier les TU et le recalcul de tout les notebooks et scripts.
# Cette règle est invoqué avant un commit sur la branche master de git.
.PHONY: validate
.make-validate: $(REQUIREMENTS) .make-test clean-notebooks $(DATA)/raw .make-typing notebooks/* # build/html build/linkcheck
	@date >.make-validate
## Validate the version before release
validate: .make-validate


## Install the tools in conda env
install: $(CONDA_PREFIX)/bin/$(PRJ)

## Install the tools in conda env with 'develop' link
develop:
	@python $(PYTHON_ARGS) setup.py develop

## Uninstall the tools from the conda env
uninstall: $(CONDA_PREFIX)/bin/$(PRJ)
	@rm $(CONDA_PREFIX)/bin/$(PRJ)


## Publish the distribution in a local repository
local-repository:
	@pip install pypiserver || true
	mkdir -p .repository/$(PRJ)
	( cd .repository/$(PRJ) ; ln -fs ../../dist/*.whl . )
	echo -e "$(green)export PIP_EXTRA_INDEX_URL=http://localhost:8888/simple$(normal)"
	echo -e "or use $(green)pip install --index-url http://localhost:8888/simple/$(normal)"
	pypi-server -p 8888 .repository/

docker-run:
	docker run -i -v /home/pprados/workspace.bda/virtual_dataframe:/virtual_dataframe \
		--runtime=nvidia \
		-t conda/miniconda3 /bin/bash
