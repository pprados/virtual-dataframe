#!/usr/bin/env bash

NAME=vdf

VDF_MODES=(
  pandas
  cudf
  modin
  dask-local
  dask_modin-local
  dask_cudf-local
  pyspark-local
  pyspark_gpu-local
  all
)

source $CONDA_HOME/bin/activate
CONDA=$CONDA_HOME/bin/conda
PYTHON_VERSION='3.9.*'
export USE_GPU

normal=$(tput sgr0)
bold=$(tput bold)
red=$(tput setaf 1)

test_mode() {
    local mode=$1
    echo -e "\n${bold}Test conda env $NAME-$mode...${normal}"
    if ! [[ "$mode" =~ ^(cudf|dask_cudf-local|pyspark_gpu-local)$ ]];
    then
      USE_GPU=false
    else
      USE_GPU=true
    fi
    conda activate $NAME-$mode
    mkdir -p ${CONDA_PREFIX}/lib/python${PYTHON_VERSION}/site-packages
    touch ${CONDA_PREFIX}/lib/python${PYTHON_VERSION}/site-packages
    $CONDA install pytest sqlalchemy python-graphviz openpyxl pytables -y
    pip uninstall virtual_dataframe -y  # Remove old version
    pip install -e . # Install current version
    make VENV=$NAME-$mode dependencies
    python -m pytest --rootdir=. -s tests/test_vnumpy.py tests/test_vpandas.py tests/test_vclient.py
    err=$?
    conda deactivate

    if [ $err -eq 0 ]; then
      echo -e "${bold}Test conda env $NAME-$mode ok${normal}\n"
    else
      echo -e "${red}Test conda env $NAME-$mode bad${normal}\n"
      exit $err
    fi
    echo -e "\n\n"
}

for mode in ${VDF_MODES[@]}
do
  if [[ $mode == "all" ]];
  then
    echo -e "\n${bold}Test conda env $NAME-$mode...${normal}"
    conda activate $NAME-$mode
    mkdir -p ${CONDA_PREFIX}/lib/python${PYTHON_VERSION}/site-packages
    touch ${CONDA_PREFIX}/lib/python${PYTHON_VERSION}/site-packages
    $CONDA install pytest sqlalchemy python-graphviz openpyxl pytables -y
    pip uninstall virtual_dataframe -y  # Remove old version
    pip install -e . # Install current version
    make VENV=$NAME-$mode dependencies

    for all_mode in ${VDF_MODES[@]}
    do
      if [[ $all_mode != "all" ]] ;
      then
        m=${all_mode%-local}
        echo "test all-${m}"
        export VDF_MODE=$m
        python -m pytest --rootdir=. -s tests/test_vnumpy.py tests/test_vpandas.py tests/test_vclient.py
        err=$?
        if [[ $err == 0 ]]; then
          echo -e "${bold}Test conda env $NAME-$mode with $m ok${normal}\n"
        else
          echo -e "${red}Test conda env $NAME-$mode with $m bad${normal}\n"
          conda deactivate
          exit $err
        fi
      fi
    done
    conda deactivate
  else
    test_mode $mode
  fi
done
