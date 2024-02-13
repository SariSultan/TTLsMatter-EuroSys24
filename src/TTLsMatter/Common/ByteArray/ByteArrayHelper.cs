/*
 *
 * Copyright (c) Sari Sultan (sarisultan@ieee.org | sari.sultan@mail.utoronto.ca)
 *
 * Part of the artifact evaluation code for Sultan et al.'s EuroSys'24 paper titled:
 * TTLs Matter: Efficient Cache Sizing with TTL-Aware Miss Ratio Curves and Working Set Sizes
 *
 * If you have any questions or want to report a bug please feel free to contact me anytime : )
 * If you want to optimize the code, please make a pull request and I will make sure to check it out, thanks.
 *
 */

using System.Runtime.CompilerServices;

namespace TTLsMatter.Common.ByteArray;

public static class ByteArrayHelper
{

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt32(byte[] buffer, int startIndex, UInt32 number)
    {
        Array.Copy(BitConverter.GetBytes(number), 0, buffer, startIndex, sizeof(UInt32));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt64(byte[] buffer, int startIndex, UInt64 number)
    {
        Array.Copy(BitConverter.GetBytes(number), 0, buffer, startIndex, sizeof(UInt64));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt16(byte[] buffer, int startIndex, UInt16 number)
    {
        Array.Copy(BitConverter.GetBytes(number), 0, buffer, startIndex, sizeof(UInt16));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteByte(byte[] buffer, int startIndex, byte byteVal)
    {
        buffer[startIndex] = byteVal;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt32(byte[] buffer, int startIndex, int number)
    {
        Array.Copy(BitConverter.GetBytes(number), 0, buffer, startIndex, sizeof(int));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64(byte[] buffer, int startIndex, long number)
    {
        Array.Copy(BitConverter.GetBytes(number), 0, buffer, startIndex, sizeof(long));
    }

   
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(byte[] buffer, int index)
    {
        return BitConverter.ToInt32(buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte ReadByte(byte[] buffer, int index)
    {
        return buffer[index];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static UInt16 ReadUInt16(byte[] buffer, int index)
    {
        return BitConverter.ToUInt16(buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Int16 ReadInt16(byte[] buffer, int index)
    {
        return BitConverter.ToInt16(buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32(byte[] buffer, int index)
    {
        return BitConverter.ToUInt32(buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(byte[] buffer, int index)
    {
        return BitConverter.ToInt64(buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ReadUInt64(byte[] buffer, int index)
    {
        return BitConverter.ToUInt64(buffer, index);
    }
   
}