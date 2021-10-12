#!/bin/bash -ex
#
# cephfs_mirror_ha.sh - test cephfs-mirror daemons in HA mode
#

PRIMARY_FS='dc'
BACKUP_FS='dc-backup'

REPO=ceph-qa-suite
REPO_DIR=ceph_repo
REPO_PATH_PFX="$REPO_DIR/$REPO"

NR_DIRECTORIES=4
NR_SNAPSHOTS=4
MIRROR_SUBDIR='/mirror'

exec_git_cmd()
{
    local arg=("$@")
    local repo_name=${arg[0]}
    local cmd=${arg[@]:1}
    git --git-dir "$repo_name/.git" $cmd
}

clone_repo()
{
    local repo_name=$1
    git clone --branch giant "http://github.com/ceph/$REPO" $repo_name
}

setup_repos()
{
    mkdir "$REPO_DIR"

    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        mkdir $repo_name
        clone_repo $repo_name
        #arr=($repo_name "config" "pull.rebase" "true")
        #exec_git_cmd "${arr[@]}"
    done
}

configure_peer()
{
    ceph mgr module enable mirroring
    ceph fs snapshot mirror enable $PRIMARY_FS
    ceph fs snapshot mirror peer_add $PRIMARY_FS client.mirror_remote@ceph $BACKUP_FS

    ceph fs snapshot mirror show distribution $PRIMARY_FS

    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        ceph fs snapshot mirror add $PRIMARY_FS "$MIRROR_SUBDIR/$repo_name"
    done
}

create_snaps()
{
    echo "==== $PWD ==="
    findmnt
    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        for j in `seq 1 $NR_SNAPSHOTS`
        do
            arr=($repo_name "pull")
            exec_git_cmd "${arr[@]}"
            r=$(( $RANDOM % 100 + 5 ))
            arr=($repo_name "reset" "--hard" "HEAD~$r")
            exec_git_cmd "${arr[@]}"
            mkdir "$repo_name/.snap/snap_$j"
        done
    done
}

unset CEPH_CLI_TEST_DUP_COMMAND

# setup git repos to be used as data set
setup_repos

# turn on mirroring, add peers...
configure_peer

# snapshots on primary
create_snaps
