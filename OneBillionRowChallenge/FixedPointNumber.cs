namespace OneBillionRowChallenge;

public static class FixedPointNumber
{
    public const int Scale = 10;
    private static readonly byte DecimalSeparator = Convert.ToByte('.');
    private static readonly byte ZeroChar = Convert.ToByte('0');
    private static readonly byte MinusChar = Convert.ToByte('-');

    public static long FromSpan(ref Span<byte> span)
    {
        var separatorPosition = span.IndexOf(DecimalSeparator);
        var sign = span[0] == MinusChar ? -1 : 1;

        var integerSpan = sign == -1 ? span[1..separatorPosition] : span[..separatorPosition];
        var decimalSpan = span[(separatorPosition + 1)..];

        var integerPart = 0;

        switch (integerSpan.Length)
        {
            case 1:
                integerPart = integerSpan[0] - ZeroChar;
                break;
            case 2:
                integerPart = (integerSpan[0] - ZeroChar) * 10;
                integerPart += integerSpan[1] - ZeroChar;
                break;
        }

        var decimalPart = decimalSpan[0] - ZeroChar;

        return sign * (integerPart * Scale + decimalPart);
    }
}