namespace TTLsMatter.Datasets.Common.Types;

/// <summary>
/// The request type read from the trace
///
/// See: https://github.com/memcached/memcached/wiki/Commands
/// </summary>
public enum RequestType : byte
{
    FilteredtwitterGetonly=0,
    Get = 1, //should start from 1
    Gets = 2,
    Set = 3,
    Add = 4,
    Replace = 5,
    Cas = 6,
    Append = 7,
    Prepend = 8,
    Delete = 9,
    Incr = 10,
    Decr = 11
}