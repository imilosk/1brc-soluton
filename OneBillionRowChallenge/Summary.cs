namespace OneBillionRowChallenge;

public struct Summary
{
    public readonly string Name;
    public long Min;
    public long Max;
    public long Sum;
    public nint Count;

    public Summary(
        string name,
        long min,
        long max,
        long sum,
        nint count
    )
    {
        Name = name;
        Min = min;
        Max = max;
        Sum = sum;
        Count = count;
    }
}