#Requires -Version 5.1
param(
    [string]$AgentPath = (Join-Path $PSScriptRoot 'relayweave-agent.exe'),
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'agent.json')
)

$ErrorActionPreference = 'Stop'
$AgentPath = (Resolve-Path -LiteralPath $AgentPath).ProviderPath
$ConfigPath = (Resolve-Path -LiteralPath $ConfigPath).ProviderPath

# WinINet flags control manual proxy, PAC and automatic detection together.
if (-not ('RelayWeaveProxy' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class RelayWeaveProxy
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    private struct Value {
        [FieldOffset(0)] public uint Number;
        [FieldOffset(0)] public IntPtr Pointer;
    }
    [StructLayout(LayoutKind.Sequential)]
    private struct Option { public uint Id; public Value Value; }
    [StructLayout(LayoutKind.Sequential)]
    private struct OptionList {
        public uint Size;
        public IntPtr Connection;
        public uint Count;
        public uint Error;
        public IntPtr Options;
    }
    [DllImport("wininet.dll", SetLastError = true)]
    private static extern bool InternetQueryOptionW(IntPtr handle, uint option, ref OptionList list, ref uint size);
    [DllImport("wininet.dll", SetLastError = true)]
    private static extern bool InternetSetOptionW(IntPtr handle, uint option, ref OptionList list, uint size);
    [DllImport("wininet.dll", EntryPoint = "InternetSetOptionW", SetLastError = true)]
    private static extern bool Notify(IntPtr handle, uint option, IntPtr buffer, uint size);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);
    public static void EnableCtrlC() { Check(SetConsoleCtrlHandler(IntPtr.Zero, false)); }
    private static void Check(bool success) {
        if (!success) throw new Win32Exception(Marshal.GetLastWin32Error());
    }
    public static uint Flags(int flags) {
        var options = new Option[1];
        options[0].Id = flags < 0 ? 10u : 1u;
        options[0].Value.Number = (uint)flags;
        var pinned = GCHandle.Alloc(options, GCHandleType.Pinned);
        try {
            var list = new OptionList {
                Size = (uint)Marshal.SizeOf(typeof(OptionList)), Count = 1,
                Options = pinned.AddrOfPinnedObject()
            };
            uint size = list.Size;
            if (flags < 0) Check(InternetQueryOptionW(IntPtr.Zero, 75, ref list, ref size));
            else {
                Check(InternetSetOptionW(IntPtr.Zero, 75, ref list, size));
                Check(Notify(IntPtr.Zero, 95, IntPtr.Zero, 0));
                Check(Notify(IntPtr.Zero, 37, IntPtr.Zero, 0));
            }
            return options[0].Value.Number;
        }
        finally { pinned.Free(); }
    }
}
'@
}

$key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings'
$flags = [RelayWeaveProxy]::Flags(-1)
$current = Get-ItemProperty -LiteralPath $key
$saved = @{}
foreach ($name in @('ProxyServer', 'ProxyOverride')) {
    $property = $current.PSObject.Properties[$name]
    $saved[$name] = if ($null -ne $property) { $property.Value } else { $null }
}

$exitCode = 1
try {
    Set-ItemProperty -LiteralPath $key -Name ProxyServer -Type String -Value 'http=127.0.0.1:8080;https=127.0.0.1:8080'
    Set-ItemProperty -LiteralPath $key -Name ProxyOverride -Type String -Value 'localhost;127.*;[::1];<local>'
    [RelayWeaveProxy]::Flags(3) | Out-Null
    Write-Host 'HTTP/HTTPS proxy: 127.0.0.1:8080. Press Ctrl+C to stop and restore settings.'
    [RelayWeaveProxy]::EnableCtrlC()
    & $AgentPath $ConfigPath
    $exitCode = $LASTEXITCODE
}
finally {
    try {
        foreach ($name in $saved.Keys) {
            if ($null -eq $saved[$name]) {
                Remove-ItemProperty -LiteralPath $key -Name $name -ErrorAction SilentlyContinue
            }
            else {
                Set-ItemProperty -LiteralPath $key -Name $name -Type String -Value $saved[$name]
            }
        }
    }
    finally {
        [RelayWeaveProxy]::Flags($flags) | Out-Null
        Write-Host 'Original Windows proxy settings restored.'
    }
}
exit $exitCode
