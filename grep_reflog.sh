
git reflog --format=format:%H | while read line 
do
    git show $line | grep $@
done
