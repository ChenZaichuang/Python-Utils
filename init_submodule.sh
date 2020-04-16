submodule_name=$1
git_repo=$2
git rm -r --cached ${submodule_name}
rm -rf .git/modules/${submodule_name}
rm -r ${submodule_name}
git submodule add ${git_repo} ${submodule_name}
./${submodule_name}/init.sh