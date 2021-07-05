using EnvDTE;
using EnvDTE80;
using Microsoft.VisualStudio.Shell;
using System;

class Utils
{
    public static void ModifyText(AsyncPackage package, Func<string, string> modify)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        var dte = (DTE2)package.GetServiceAsync(typeof(DTE)).GetAwaiter().GetResult();

        string fileExtension = System.IO.Path.GetExtension(dte.ActiveDocument.FullName);
        bool isSqlFile = fileExtension.ToUpper().Equals(".SQL");

        if (isSqlFile)
        {
            string fullText = SelectAllCodeFromDocument(dte.ActiveDocument);
            TextSelection selection = (TextSelection)dte.ActiveDocument.Selection;
            if (!selection.IsActiveEndGreater)
                selection.SwapAnchor();
            string selectionText = selection.Text;
            bool formatSelectionOnly = selectionText.Length > 0 && selectionText.Length != fullText.Length;
            int cursorPoint = selection.ActivePoint.AbsoluteCharOffset;

            string textToFormat = formatSelectionOnly ? selectionText : fullText;

            string formattedText = modify(textToFormat);

            if (formatSelectionOnly)
            {
                selection.Insert(formattedText, (int)EnvDTE.vsInsertFlags.vsInsertFlagsContainNewText);
            }
            else
            {
                //if whole doc then replace all text, and put the cursor approximately where it was (using proportion of text total length before and after)
                int newPosition = textToFormat.Length == 0 ? 1 : (int)Math.Round(1.0 * cursorPoint * formattedText.Length / textToFormat.Length, 0, MidpointRounding.AwayFromZero);
                ReplaceAllCodeInDocument(dte.ActiveDocument, formattedText);
                SafelySetCursorAt(dte.ActiveDocument, newPosition);
            }
        }
    }

    public static string SelectAllCodeFromDocument(Document targetDoc)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        string outText = "";
        TextDocument textDoc = targetDoc.Object("TextDocument") as TextDocument;
        if (textDoc != null)
            outText = textDoc.StartPoint.CreateEditPoint().GetText(textDoc.EndPoint);
        return outText;
    }

    public static void ReplaceAllCodeInDocument(Document targetDoc, string newText)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        TextDocument textDoc = targetDoc.Object("TextDocument") as TextDocument;
        if (textDoc != null)
        {
            textDoc.StartPoint.CreateEditPoint().ReplaceText(textDoc.EndPoint, newText, 0);
        }
    }

    private static void SafelySetCursorAt(Document targetDoc, int newPosition)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        TextDocument textDoc = targetDoc.Object("TextDocument") as TextDocument;
        if (textDoc != null)
        {
            var textEndPoint = textDoc.EndPoint.AbsoluteCharOffset;
            if (textEndPoint < newPosition)
                newPosition = textEndPoint;
        }

        ((TextSelection)(targetDoc.Selection)).MoveToAbsoluteOffset(newPosition, false);
    }
}