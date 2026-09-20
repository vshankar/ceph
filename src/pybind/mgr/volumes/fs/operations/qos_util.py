import errno
import json

import cephfs

from ..exception import VolumeException

QOS_XATTR = "ceph.dir.qos"
QOS_EFFECTIVE_XATTR = "ceph.dir.qos.effective"


def _parse_param(name, value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise VolumeException(-errno.EINVAL,
                              "qos {0} must be an integer".format(name))
    if value < 1:
        raise VolumeException(-errno.EINVAL,
                              "qos {0} must be greater than zero".format(name))
    return value


def qos_set(fs, path, reservation, weight, limit):
    """
    Set the dmclock QoS parameters on a directory. The settings are stored in
    the directory inode by the MDS, so they survive an MDS restart or failover,
    and are inherited by everything below the directory.
    """
    qos = {
        "reservation": _parse_param("reservation", reservation),
        "weight": _parse_param("weight", weight),
        "limit": _parse_param("limit", limit),
    }

    if qos["reservation"] > qos["limit"]:
        raise VolumeException(-errno.EINVAL,
                              "qos reservation cannot be greater than limit")

    try:
        fs.setxattr(path, QOS_XATTR, json.dumps(qos).encode('utf-8'), 0)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

    return json.dumps(qos, indent=4, sort_keys=True)


def qos_rm(fs, path):
    """
    Remove the dmclock QoS parameters from a directory. QoS set on an ancestor
    (e.g. the subvolume group) applies again once this is removed.
    """
    try:
        fs.removexattr(path, QOS_XATTR, 0)
    except cephfs.NoData:
        pass
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])


def _getxattr_qos(fs, path, xattr):
    try:
        value = fs.getxattr(path, xattr).decode('utf-8')
    except cephfs.NoData:
        return None
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

    try:
        # the MDS wraps the parameters in a "qos" object
        return json.loads(value)["qos"]
    except (ValueError, KeyError, TypeError):
        raise VolumeException(-errno.EINVAL,
                              "malformed qos metadata: {0}".format(value))


def qos_get(fs, path):
    """
    Get the dmclock QoS parameters in effect for a directory. If nothing is set
    on the directory itself the value inherited from an ancestor is reported
    instead, flagged with "inherited".
    """
    qos = _getxattr_qos(fs, path, QOS_XATTR)
    if qos is None:
        qos = _getxattr_qos(fs, path, QOS_EFFECTIVE_XATTR)
        if qos is None:
            return json.dumps({}, indent=4)
        qos["inherited"] = True

    return json.dumps(qos, indent=4, sort_keys=True)
