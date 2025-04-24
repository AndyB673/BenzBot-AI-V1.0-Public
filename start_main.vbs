On Error Resume Next

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

' Ermittelt den Ordner, in dem dieses Skript liegt
currentDir = fso.GetParentFolderName(WScript.ScriptFullName)
mainPy = currentDir & "\main.py"

' Überprüfe, ob main.py existiert
If fso.FileExists(mainPy) Then
    ' Baut den Befehl zum Starten von main.py aus dem aktuellen Ordner
    command = "cmd /c start /B python """ & mainPy & """"
    WshShell.Run command, 0, False

    If Err.Number <> 0 Then
        MsgBox "Fehler beim Starten von main.py: " & Err.Description, vbExclamation, "Fehler"
        Err.Clear
    Else
        ' Optional: Erfolgsmeldung anzeigen
        ' MsgBox "main.py wurde asynchron gestartet.", vbInformation, "Erfolg"
    End If
Else
    MsgBox "Die Datei main.py wurde nicht gefunden: " & mainPy, vbExclamation, "Dateifehler"
End If

On Error GoTo 0