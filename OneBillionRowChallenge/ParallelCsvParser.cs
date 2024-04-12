using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace OneBillionRowChallenge;

public static class ParallelCsvParser
{
    private const int BufferSize = 2097152; // 2048Kb
    private const int HashTableSize = 32768; // 2 ^ 15

    public static void Parse(string filePath)
    {
        var chunkCount = Environment.ProcessorCount;
        var dictionaries = InitDictionaries(chunkCount);

        var safeFileHandle = File.OpenHandle(filePath);
        var fileLength = RandomAccess.GetLength(safeFileHandle);

        var chunkStartOffsets = CalculateChunkStartOffsets(safeFileHandle, chunkCount);

        ScheduleWorkToThreads(chunkStartOffsets, fileLength, safeFileHandle, chunkCount, dictionaries);

        var mergedDictionary = MergeDictionaries(dictionaries);

        PrintResults(mergedDictionary);
    }

    private static Dictionary<uint, Summary>[] InitDictionaries(int chunkCount)
    {
        var dictionaries = new Dictionary<uint, Summary>[chunkCount];
        for (var i = 0; i < dictionaries.Length; i++)
        {
            dictionaries[i] = new Dictionary<uint, Summary>(HashTableSize);
        }

        return dictionaries;
    }

    private static void ScheduleWorkToThreads(
        long[] chunkStartOffsets,
        long fileLength,
        SafeFileHandle safeFileHandle,
        int numberOfThreads,
        Dictionary<uint, Summary>[] dictionaries
    )
    {
        var threads = new Thread[numberOfThreads];
        for (var i = 0; i < numberOfThreads; i++)
        {
            var chunkStart = chunkStartOffsets[i];
            var chunkEnd = i + 1 < numberOfThreads ? chunkStartOffsets[i + 1] : fileLength;
            var chunkSize = chunkEnd - chunkStart;

            var i1 = i;
            threads[i] = new Thread(() => { ChunkProcessor(safeFileHandle, chunkStart, chunkSize, i1, dictionaries); });
        }

        foreach (var thread in threads)
        {
            thread.Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }
    }

    private static void ChunkProcessor(SafeFileHandle sfh, long chunkStart, long chunkSize, int chunkNumber,
        Dictionary<uint, Summary>[] dictionaries)
    {
        var results = dictionaries[chunkNumber];

        var buffer = new byte[BufferSize];
        int bytesRead;
        var totalBytes = 0L;

        while ((bytesRead = RandomAccess.Read(sfh, buffer, chunkStart + totalBytes)) > 0)
        {
            totalBytes += bytesRead;
            var offset = ProcessBuffer(buffer.AsSpan()[..bytesRead], results);
            totalBytes -= offset;

            if (totalBytes >= chunkSize)
            {
                break;
            }
        }
    }

    private static long ProcessBuffer(Span<byte> buffer, Dictionary<uint, Summary> results)
    {
        var position = 0;
        var delimiterPosition = 0;
        var currentLineLength = 0;
        for (var index = 0; index < buffer.Length; index++)
        {
            var c = buffer[index];

            currentLineLength++;
            if (c == Constants.Delimiter)
            {
                delimiterPosition = currentLineLength - 1;
                continue;
            }

            if (c != Constants.NewLine)
            {
                continue;
            }

            var line = buffer[position..index];

            AddMeasurement(ref line, delimiterPosition, results);

            position = index + 1;
            currentLineLength = 0;
        }

        return currentLineLength;
    }

    private static void AddMeasurement(ref Span<byte> line, int delimiterPosition, Dictionary<uint, Summary> results)
    {
        var stationSpan = line[..delimiterPosition];
        var temperatureSpan = line[(delimiterPosition + 1)..];

        var stationHash = Utils.CustomHash(ref stationSpan);
        var temperature = FixedPointNumber.FromSpan(ref temperatureSpan);

        ref var stationSummary = ref CollectionsMarshal.GetValueRefOrNullRef(results, stationHash);

        if (Unsafe.IsNullRef(ref stationSummary))
        {
            results.Add(stationHash, new Summary(
                Encoding.UTF8.GetString(stationSpan),
                temperature,
                temperature,
                temperature,
                1
            ));

            return;
        }

        stationSummary.Min = temperature < stationSummary.Min ? temperature : stationSummary.Min;
        stationSummary.Max = temperature > stationSummary.Max ? temperature : stationSummary.Max;

        stationSummary.Sum += temperature;
        stationSummary.Count++;
    }

    private static long[] CalculateChunkStartOffsets(SafeFileHandle safeFileHandle, int chunkCount)
    {
        var fileLength = RandomAccess.GetLength(safeFileHandle);
        var chunkStartOffsets = new long[chunkCount];
        var buffer = new byte[1];

        for (var i = 1; i < chunkStartOffsets.Length; i++)
        {
            var start = fileLength * i / chunkStartOffsets.Length;
            var offset = 0;

            do
            {
                RandomAccess.Read(safeFileHandle, buffer, start + offset);
                offset++;
            } while (buffer[0] != Constants.NewLine);

            start += offset;
            chunkStartOffsets[i] = start;
        }

        return chunkStartOffsets;
    }

    private static Dictionary<uint, Summary> MergeDictionaries(Dictionary<uint, Summary>[] dictionaries)
    {
        var mergedDictionary = new Dictionary<uint, Summary>(HashTableSize);
        foreach (var dictionary in dictionaries)
        {
            foreach (var (key, existingValue) in dictionary)
            {
                ref var stationSummary = ref CollectionsMarshal.GetValueRefOrNullRef(mergedDictionary, key);

                if (Unsafe.IsNullRef(ref stationSummary))
                {
                    mergedDictionary[key] = existingValue;

                    continue;
                }

                stationSummary.Min = Math.Min(stationSummary.Min, existingValue.Min);
                stationSummary.Max = Math.Max(stationSummary.Max, existingValue.Max);
                stationSummary.Sum += existingValue.Sum;
                stationSummary.Count += existingValue.Count;
            }
        }

        return mergedDictionary;
    }

    private static void PrintResults(Dictionary<uint, Summary> results)
    {
        var customCulture =
            (System.Globalization.CultureInfo)Thread.CurrentThread.CurrentCulture.Clone();
        customCulture.NumberFormat.NumberDecimalSeparator = ".";
        Thread.CurrentThread.CurrentCulture = customCulture;

        var sortedDict = results.OrderBy(pair => pair.Value.Name, StringComparer.Ordinal).ToList();

        const double point = FixedPointNumber.Scale;

        Console.Write("{");
        for (var i = 0; i < sortedDict.Count; i++)
        {
            var kvp = sortedDict.ElementAt(i);
            var value = kvp.Value;

            var average = value.Sum / point / value.Count;

            Console.Write("{0}={1:F1}/{2:F1}/{3:F1}", value.Name, value.Min / point, average, value.Max / point);

            if (i < results.Count - 1)
            {
                Console.Write(", ");
            }
        }

        Console.WriteLine("}");
    }
}