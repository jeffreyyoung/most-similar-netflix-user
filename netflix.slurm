#!/bin/bash

#Submit this script with: sbatch wordcount.slurm

#SBATCH --time=00:10:00   # walltime
#SBATCH --ntasks=16 # number of processor cores (i.e. tasks)
#SBATCH --nodes=1   # number of nodes
#SBATCH --ntasks-per-node=16   # make sure there's 16 processors per node
#SBATCH --mem-per-cpu=2048M   # memory per CPU core
#SBATCH -J "Hadoop"   # job name

# The script hadoop.py will initialize a Hadoop cluster on the nodes given by slurm.
# The PBS_NODEFILE variable is needed for the script to determine the nodelist given by slurm.
# It is a good idea to reserve all of the processors on the node with the parameter
#   --ntasks-per-node as seen above.
# You should also request a minimum of 2 nodes.

# Compatibility variables for PBS. hadoop.py uses this variable to determine which nodes should appear in the cluster.
export PBS_NODEFILE=`/fslapps/fslutils/generate_pbs_nodefile`
#
# These variables are set just to shorten the command below.
export HADOOP_GROUP=/fslgroup/fslg_hadoop
export HADOOP_PATH=/fslgroup/fslg_hadoop/hadoop-1.1.2
#
# hadoop.py will pass the quoted string into the typical hadoop command.
# When referencing your home directory the variable ${HOME} should be used rather than '~' to ensure proper substitution.
#  If ~ appears in a quoted string, it will not be substituted.  This could cause hadoop to look for a folder called '~'
#  rather than your home directory.
$HADOOP_PATH/bin/hadoop.py "jar ${HOME}/most-similar-netflix-user/Netflix.jar Netflix ${HOME}/most-similar-netflix-user/testinput.txt ${HOME}/compute/most-similar-netflix-user/output1 ${HOME}/compute/most-similar-netflix-user/output2 30878"

exit 0
