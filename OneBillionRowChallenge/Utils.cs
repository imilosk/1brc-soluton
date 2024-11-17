namespace OneBillionRowChallenge;

public static class Utils
{
    public static nint CustomHash(ref Span<byte> key)
    {
        nint h1 = 0;

        foreach (var b in key)
        {
            h1 ^= b;
            h1 *= 0x5bd1e995;
            h1 ^= h1 >> 24;
        }

        return h1;
    }
}