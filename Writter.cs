using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQL_Formatter
{
    public interface ISQLWritter
    {
        void Keyword(string keyword);

        void Text(string text);

        void Space(bool canBreak = false);

        void NewLine();

        void IndentToCursor();

        void Indent();

        void DeIndent();
    }

    public static class Extensions
    {
        public static void Join<T>(this ISQLWritter writter, IList<T> items, TSqlFragmentVisitor visitor) where T : TSqlFragment
        {
            writter.Join(
                separator: next =>
                {
                    writter.Text(",");
                    PrimaryExpression expression = null;
                    switch (next)
                    {
                        case PrimaryExpression primary:
                            expression = primary;
                            break;
                        case SelectScalarExpression select when (select.Expression is PrimaryExpression primary):
                            expression = primary;
                            break;
                        case ExpressionGroupingSpecification grouping when (grouping.Expression is PrimaryExpression primary):
                            expression = primary;
                            break;
                        case ExpressionWithSortOrder sort when (sort.Expression is PrimaryExpression primary):
                            expression = primary;
                            break;
                    };

                    if (expression is CaseExpression || expression is ScalarSubquery)
                        writter.NewLine();
                    else
                        writter.Space(canBreak: next is SelectScalarExpression || next is ExpressionGroupingSpecification || next is ExpressionWithSortOrder);
                },
                items,
                visitor
            );
        }

        public static void Join<T>(this ISQLWritter writter, Action<T> separator, IList<T> items, TSqlFragmentVisitor visitor) where T : TSqlFragment
        {
            writter.Join(separator, items, onEach: item => item.Accept(visitor));
        }

        public static void Join<T>(this ISQLWritter writter, Action<T> separator, IList<T> items, Action<T> onEach) where T : TSqlFragment
        {
            writter.IndentToCursor(() =>
            {
                for (int i = 0; i < items.Count; i++)
                {
                    onEach(items[i]);
                    if (i < items.Count - 1)
                        separator(items[i + 1]);
                }
            });
        }

        public static void IndentToCursor(this ISQLWritter writter, Action block)
        {
            writter.IndentToCursor();
            block();
            writter.DeIndent();
        }

        public static void Indent(this ISQLWritter writter, Action block)
        {
            writter.Indent();
            block();
            writter.DeIndent();
        }
    }

    public class SQLWritter : ISQLWritter
    {
        public string Sql { get; private set; } = "";
        public int IndentCount = 4;

        public int MaxColumnSize = 80;

        private int CursorPosition()
        {
            return Sql.Length - Sql.LastIndexOf('\n') - 1;
        }

        public void Keyword(string keyword)
        {
            Sql += keyword.ToUpper();
        }

        public void NewLine()
        {
            Sql += "\n" + new String(' ', Indents.Last());
        }

        public void Space(bool canBreak = false)
        {
            if (canBreak && CursorPosition() >= MaxColumnSize)
            {
                NewLine();
            }
            else
            {
                Sql += " ";
            }
        }

        public void Text(string text)
        {
            Sql += text;
        }

        private IList<int> Indents = new List<int>() { 0 };

        public void IndentToCursor()
        {
            Indents.Add(CursorPosition());
        }

        public void Indent()
        {
            var currentIndent = Indents.Last();
            Indents.Add(currentIndent + IndentCount);
        }

        public void DeIndent()
        {
            Indents.RemoveAt(Indents.Count - 1);
        }
    }
}