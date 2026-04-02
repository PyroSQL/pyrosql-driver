using System.Collections;
using System.Data.Common;

namespace PyroSQL.Data;

/// <summary>
/// A collection of PyroSqlParameter objects.
/// </summary>
public sealed class PyroSqlParameterCollection : DbParameterCollection
{
    private readonly List<PyroSqlParameter> _parameters = new();
    private readonly object _syncRoot = new();

    public override int Count => _parameters.Count;
    public override object SyncRoot => _syncRoot;

    public new PyroSqlParameter this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = value ?? throw new ArgumentNullException(nameof(value));
    }

    public new PyroSqlParameter this[string parameterName]
    {
        get => _parameters[IndexOfChecked(parameterName)];
        set => _parameters[IndexOfChecked(parameterName)] = value ?? throw new ArgumentNullException(nameof(value));
    }

    public override int Add(object value)
    {
        var param = (PyroSqlParameter)value;
        _parameters.Add(param);
        return _parameters.Count - 1;
    }

    public PyroSqlParameter Add(string parameterName, object? value)
    {
        var param = new PyroSqlParameter(parameterName, value);
        _parameters.Add(param);
        return param;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values)
            Add(v);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((PyroSqlParameter)value);

    public override bool Contains(string value) => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index)
    {
        ((ICollection)_parameters).CopyTo(array, index);
    }

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => _parameters.IndexOf((PyroSqlParameter)value);

    public override int IndexOf(string parameterName)
    {
        for (int i = 0; i < _parameters.Count; i++)
        {
            if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    public override void Insert(int index, object value)
    {
        _parameters.Insert(index, (PyroSqlParameter)value);
    }

    public override void Remove(object value)
    {
        _parameters.Remove((PyroSqlParameter)value);
    }

    public override void RemoveAt(int index)
    {
        _parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        int idx = IndexOfChecked(parameterName);
        _parameters.RemoveAt(idx);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        return _parameters[IndexOfChecked(parameterName)];
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        _parameters[index] = (PyroSqlParameter)value;
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        _parameters[IndexOfChecked(parameterName)] = (PyroSqlParameter)value;
    }

    private int IndexOfChecked(string parameterName)
    {
        int idx = IndexOf(parameterName);
        if (idx < 0)
            throw new ArgumentException($"Parameter '{parameterName}' not found.", nameof(parameterName));
        return idx;
    }

    internal IReadOnlyList<PyroSqlParameter> InternalList => _parameters;
}
