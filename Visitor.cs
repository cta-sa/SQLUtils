using System;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQL_Formatter
{
    public class SQLVisitor : TSqlFragmentVisitor
    {
        private ISQLWritter Writter;

        public SQLVisitor(ISQLWritter writter)
        {
            Writter = writter;
        }

        override public void Visit(TSqlFragment node)
        {
            var substring = String.Join("", node.ScriptTokenStream.Skip(node.FirstTokenIndex).Take(node.LastTokenIndex - node.FirstTokenIndex + 1).Select(token => token.Text));
            throw new Exception(node.ToString() + '\n' + substring);
        }

        override public void ExplicitVisit(OverClause node)
        {
            Writter.Keyword("OVER");
            Writter.Text("(");
            Writter.IndentToCursor(() =>
            {
                if (node.Partitions.Count > 0)
                {
                    Writter.Keyword("PARTITION");
                    Writter.Space();
                    Writter.Keyword("BY");
                    Writter.Space();
                    Writter.Join(node.Partitions, visitor: this);
                }

                if (node.OrderByClause != null)
                {
                    if (node.Partitions.Count > 0)
                        Writter.Space(canBreak: true);

                    node.OrderByClause.Accept(this);
                }
            });
            //public WindowFrameClause WindowFrameClause { get; set; }
            Writter.Text(")");
        }

        override public void ExplicitVisit(AssignmentSetClause node)
        {
            if (node.Variable != null)
            {
                node.Variable.Accept(this);
                Writter.Space();
                Writter.Text("=");
                Writter.Space();
            }

            node.Column.Accept(this);
            Writter.Space();
            switch (node.AssignmentKind)
            {
                case AssignmentKind.Equals:
                    Writter.Text("=");
                    break;
                case AssignmentKind.AddEquals:
                    Writter.Text("+=");
                    break;
                case AssignmentKind.SubtractEquals:
                    Writter.Text("-=");
                    break;
                case AssignmentKind.MultiplyEquals:
                    Writter.Text("*=");
                    break;
                case AssignmentKind.DivideEquals:
                    Writter.Text("/=");
                    break;
                case AssignmentKind.ModEquals:
                    Writter.Text("%=");
                    break;
                case AssignmentKind.BitwiseAndEquals:
                    Writter.Text("&=");
                    break;
                case AssignmentKind.BitwiseOrEquals:
                    Writter.Text("|=");
                    break;
                case AssignmentKind.BitwiseXorEquals:
                    Writter.Text("^=");
                    break;
            }

            Writter.Space();
            node.NewValue.Accept(this);
        }

        override public void ExplicitVisit(UpdateSpecification node)
        {
            Writter.Keyword("UPDATE");

            if (node.TopRowFilter != null)
            {
                Writter.Space();
                node.TopRowFilter.Accept(this);
            }

            Writter.Space();
            node.Target.Accept(this);

            Writter.NewLine();
            Writter.Keyword("SET");

            Writter.Space();
            Writter.Join(separator: _ => { Writter.Text(","); Writter.NewLine(); }, node.SetClauses, visitor: this);

            if (node.WhereClause != null)
            {
                Writter.NewLine();
                node.WhereClause.Accept(this);
            }

            // public FromClause FromClause { get; set; }
            // public OutputIntoClause OutputIntoClause { get; set; }
            // public OutputClause OutputClause { get; set; }
        }

        override public void ExplicitVisit(DeleteSpecification node)
        {
            Writter.Keyword("DELETE");

            if (node.TopRowFilter != null)
            {
                Writter.Space();
                node.TopRowFilter.Accept(this);
            }

            Writter.Space();
            node.Target.Accept(this);

            if (node.WhereClause != null)
            {
                Writter.NewLine();
                node.WhereClause.Accept(this);
            }

            // public FromClause FromClause { get; set; }
            // public OutputIntoClause OutputIntoClause { get; set; }
            // public OutputClause OutputClause { get; set; }
        }

        override public void ExplicitVisit(DeleteStatement node)
        {
            ExplicitVisit((StatementWithCtesAndXmlNamespaces)node);
            node.DeleteSpecification.Accept(this);
        }

        override public void ExplicitVisit(PivotedTableReference node)
        {
            node.TableReference.Accept(this);
            Writter.NewLine();
            Writter.Keyword("PIVOT");
            Writter.Space();
            Writter.Text("(");
            node.AggregateFunctionIdentifier.Accept(this);
            Writter.Text("(");
            Writter.Join(node.ValueColumns, visitor: this);
            Writter.Text(")");
            Writter.Space();
            Writter.Keyword("FOR");
            Writter.Space();
            node.PivotColumn.Accept(this);
            Writter.Space();
            Writter.Keyword("IN");
            Writter.Text("(");
            Writter.Join(node.InColumns, visitor: this);
            Writter.Text(")");
            Writter.Text(")");
            ExplicitVisit((TableReferenceWithAlias)node);
        }

        override public void ExplicitVisit(WithinGroupClause node)
        {
            Writter.Keyword("WITHIN");
            Writter.Space();
            Writter.Keyword("GROUP");
            Writter.Space();
            Writter.Text("(");
            // public bool HasGraphPath { get; set; }
            node.OrderByClause?.Accept(this);
            Writter.Text(")");
        }

        override public void ExplicitVisit(SchemaObjectFunctionTableReference node)
        {
            node.SchemaObject.Accept(this);
            Writter.Text("(");
            Writter.Join(node.Parameters, visitor: this);
            Writter.Text(")");
            ExplicitVisit((TableReferenceWithAliasAndColumns)node);
        }

        override public void ExplicitVisit(BooleanNotExpression node)
        {
            Writter.Keyword("NOT");
            Writter.Space();
            node.Expression.Accept(this);
        }

        override public void ExplicitVisit(CastCall node)
        {
            Writter.Keyword("CAST");
            Writter.Text("(");
            node.Parameter.Accept(this);
            Writter.Space();
            Writter.Keyword("AS");
            Writter.Space();
            node.DataType.Accept(this);
            Writter.Text(")");
            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(RollbackTransactionStatement node)
        {
            Writter.Keyword("ROLLBACK");
        }

        override public void ExplicitVisit(CommitTransactionStatement node)
        {
            Writter.Keyword("COMMIT");

            if (node.DelayedDurabilityOption != OptionState.NotSet)
            {
                Writter.Space();
                Writter.Keyword("WITH");
                Writter.Text("(");
                Writter.Keyword("DELAYED_DURABILITY");
                Writter.Space();
                Writter.Text("=");
                Writter.Space();
                switch (node.DelayedDurabilityOption)
                {
                    case OptionState.Off:
                        Writter.Keyword("OFF");
                        break;
                    case OptionState.On:
                        Writter.Keyword("ON");
                        break;
                    case OptionState.Primary:
                        Writter.Keyword("PRIMARY");
                        break;
                }
                Writter.Text(")");
            }
        }

        override public void ExplicitVisit(RowValue node)
        {
            Writter.Text("(");
            Writter.Join(node.ColumnValues, visitor: this);
            Writter.Text(")");
        }

        override public void ExplicitVisit(ValuesInsertSource node)
        {
            if (node.IsDefaultValues)
            {
                Writter.Keyword("DEFAULT");
                Writter.Space();
                Writter.Keyword("VALUES");
            }
            else
            {
                Writter.Keyword("VALUES");
                Writter.Space();
                Writter.Join(separator: _ => { Writter.Text(","); Writter.NewLine(); }, node.RowValues, visitor: this);
            }
        }

        override public void ExplicitVisit(InsertSpecification node)
        {
            // public OutputIntoClause OutputIntoClause { get; set; }
            // public OutputClause OutputClause { get; set; }

            Writter.Keyword("INSERT");

            if (node.TopRowFilter != null)
            {
                Writter.Space();
                node.TopRowFilter.Accept(this);
            }

            switch (node.InsertOption)
            {
                case InsertOption.Into:
                    Writter.Space();
                    Writter.Keyword("INTO");
                    break;
                case InsertOption.Over:
                    Writter.Space();
                    Writter.Keyword("OVER");
                    break;
            }

            Writter.Space();
            node.Target.Accept(this);

            if (node.Columns.Count > 0)
            {
                Writter.Text("(");
                Writter.Join(node.Columns, visitor: this);
                Writter.Text(")");
            }

            Writter.NewLine();
            node.InsertSource.Accept(this);
        }

        override public void ExplicitVisit(UpdateStatement node)
        {
            ExplicitVisit((StatementWithCtesAndXmlNamespaces)node);
            node.UpdateSpecification.Accept(this);
        }

        override public void ExplicitVisit(InsertStatement node)
        {
            ExplicitVisit((StatementWithCtesAndXmlNamespaces)node);
            node.InsertSpecification.Accept(this);
        }

        override public void ExplicitVisit(VariableReference node)
        {
            Writter.Text(node.Name);
            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(SetVariableStatement node)
        {
            Writter.Keyword("SET");

            Writter.Space();
            node.Variable.Accept(this);

            Writter.Space();
            switch (node.AssignmentKind)
            {
                case AssignmentKind.Equals:
                    Writter.Text("=");
                    break;
                case AssignmentKind.AddEquals:
                    Writter.Text("+=");
                    break;
                case AssignmentKind.SubtractEquals:
                    Writter.Text("-=");
                    break;
                case AssignmentKind.MultiplyEquals:
                    Writter.Text("*=");
                    break;
                case AssignmentKind.DivideEquals:
                    Writter.Text("/=");
                    break;
                case AssignmentKind.ModEquals:
                    Writter.Text("%=");
                    break;
                case AssignmentKind.BitwiseAndEquals:
                    Writter.Text("&=");
                    break;
                case AssignmentKind.BitwiseOrEquals:
                    Writter.Text("|=");
                    break;
                case AssignmentKind.BitwiseXorEquals:
                    Writter.Text("^=");
                    break;
            }

            Writter.Space();
            node.Expression.Accept(this);

            // public SeparatorType SeparatorType { get; set; }
            // public Identifier Identifier { get; set; }
            // public bool FunctionCallExists { get; set; }
            // public IList<ScalarExpression> Parameters { get; }
            // public CursorDefinition CursorDefinition { get; set; }
        }

        override public void ExplicitVisit(AlterTableStatement node)
        {
            Writter.Keyword("ALTER");
            Writter.Space();
            Writter.Keyword("TABLE");
            Writter.Space();
            node.SchemaObjectName.Accept(this);
        }

        override public void ExplicitVisit(NullableConstraintDefinition node)
        {
            if (!node.Nullable)
            {
                Writter.Keyword("NOT");
                Writter.Space();
            }

            Writter.Keyword("NULL");
        }

        override public void ExplicitVisit(DeclareVariableElement node)
        {
            node.VariableName.Accept(this);

            Writter.Space();
            node.DataType.Accept(this);

            if (node.Nullable != null)
            {
                Writter.Space();
                node.Nullable.Accept(this);
            }

            Writter.Space();
            Writter.Text("=");

            Writter.Space();
            node.Value.Accept(this);
        }

        override public void ExplicitVisit(DeclareVariableStatement node)
        {
            Writter.Keyword("DECLARE");
            Writter.Space();
            Writter.Join(separator: _ => { Writter.Text(","); Writter.NewLine(); }, node.Declarations, visitor: this);
        }

        override public void ExplicitVisit(AlterTableAlterColumnStatement node)
        {
            ExplicitVisit((AlterTableStatement)node);

            Writter.Space();
            Writter.Keyword("ALTER");
            Writter.Space();
            Writter.Keyword("COLUMN");

            Writter.Space();
            node.ColumnIdentifier.Accept(this);

            Writter.Space();
            node.DataType.Accept(this);

            // public AlterTableAlterColumnOption AlterTableAlterColumnOption { get; set; }
            // public ColumnStorageOptions StorageOptions { get; set; }
            // public IList<IndexOption> Options { get; }
            // public GeneratedAlwaysType? GeneratedAlways { get; set; }
            // public bool IsHidden { get; set; }
            // public ColumnEncryptionDefinition Encryption { get; set; }
            // public Identifier Collation { get; set; }
            // public bool IsMasked { get; set; }
            // public StringLiteral MaskingFunction { get; set; }
        }

        override public void ExplicitVisit(ExistsPredicate node)
        {
            Writter.Keyword("EXISTS");
            Writter.Space();
            node.Subquery.Accept(this);
        }

        override public void ExplicitVisit(MultiPartIdentifierCallTarget node)
        {
            node.MultiPartIdentifier.Accept(this);
        }

        override public void ExplicitVisit(WhenClause node)
        {
            Writter.Space();
            Writter.Keyword("THEN");
            Writter.Space();
            Writter.IndentToCursor(() => node.ThenExpression.Accept(this));
        }

        override public void ExplicitVisit(SearchedWhenClause node)
        {
            Writter.Keyword("WHEN");
            Writter.Space();
            Writter.IndentToCursor(() => node.WhenExpression.Accept(this));

            ExplicitVisit((WhenClause)node);
        }

        override public void ExplicitVisit(SimpleWhenClause node)
        {
            Writter.Keyword("WHEN");
            Writter.Space();
            Writter.IndentToCursor(() => node.WhenExpression.Accept(this));

            ExplicitVisit((WhenClause)node);
        }

        override public void ExplicitVisit(CaseExpression node)
        {
            if (node.ElseExpression != null)
            {
                Writter.NewLine();
                Writter.Keyword("ELSE");
                Writter.Space();
                Writter.IndentToCursor(() => node.ElseExpression.Accept(this));
            }
        }

        override public void ExplicitVisit(SearchedCaseExpression node)
        {
            Writter.IndentToCursor(() =>
            {
                Writter.Keyword("CASE");

                Writter.Indent(() =>
                {
                    Writter.NewLine();
                    Writter.Join(separator: _ => Writter.NewLine(), node.WhenClauses, visitor: this);
                    ExplicitVisit((CaseExpression)node);
                });

                Writter.NewLine();
                Writter.Keyword("END");

                ExplicitVisit((PrimaryExpression)node);
            });
        }

        override public void ExplicitVisit(SimpleCaseExpression node)
        {
            Writter.IndentToCursor(() =>
            {
                Writter.Keyword("CASE");
                Writter.Space();
                node.InputExpression.Accept(this);

                Writter.Indent(() =>
                {
                    Writter.NewLine();
                    Writter.Join(separator: _ => Writter.NewLine(), node.WhenClauses, visitor: this);
                    ExplicitVisit((CaseExpression)node);
                });

                Writter.NewLine();
                Writter.Keyword("END");

                ExplicitVisit((PrimaryExpression)node);
            });
        }

        override public void ExplicitVisit(ParenthesisExpression node)
        {
            Writter.Text("(");
            Writter.IndentToCursor(() => node.Expression.Accept(this));
            Writter.Text(")");
            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(UnaryExpression node)
        {
            switch (node.UnaryExpressionType)
            {
                case UnaryExpressionType.Positive:
                    Writter.Text("+");
                    break;
                case UnaryExpressionType.Negative:
                    Writter.Text("-");
                    break;
                case UnaryExpressionType.BitwiseNot:
                    Writter.Text("~");
                    break;
            }
            node.Expression.Accept(this);
        }

        override public void ExplicitVisit(ConvertCall node)
        {
            Writter.Keyword("CONVERT");
            Writter.Text("(");
            node.DataType.Accept(this);
            Writter.Text(",");
            Writter.Space();
            node.Parameter.Accept(this);
            if (node.Style != null)
            {
                Writter.Text(",");
                Writter.Space();
                node.Style.Accept(this);
            }
            Writter.Text(")");

            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(ScalarSubquery node)
        {
            Writter.Text("(");
            Writter.IndentToCursor(() => node.QueryExpression.Accept(this));
            Writter.Text(")");
        }

        override public void ExplicitVisit(LeftFunctionCall node)
        {
            Writter.Keyword("LEFT");
            Writter.Text("(");
            Writter.Join(node.Parameters, visitor: this);
            Writter.Text(")");

            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(InPredicate node)
        {
            node.Expression.Accept(this);

            if (node.NotDefined)
            {
                Writter.Space();
                Writter.Keyword("NOT");
            }

            Writter.Space();
            Writter.Keyword("IN");

            if (node.Subquery != null)
            {
                Writter.Space();
                node.Subquery.Accept(this);
            }


            if (node.Values.Count > 0)
            {
                Writter.Space();
                Writter.Text("(");
                Writter.Join(node.Values, visitor: this);
                Writter.Text(")");
            }
        }

        override public void ExplicitVisit(DataTypeReference node)
        {
            node.Name.Accept(this);
        }

        override public void ExplicitVisit(ParameterizedDataTypeReference node)
        {
            ExplicitVisit((DataTypeReference)node);
            if (node.Parameters.Count > 0)
            {
                Writter.Text("(");
                Writter.Join(node.Parameters, visitor: this);
                Writter.Text(")");
            }
        }

        override public void ExplicitVisit(SqlDataTypeReference node)
        {
            ExplicitVisit((ParameterizedDataTypeReference)node);
        }

        override public void ExplicitVisit(TryConvertCall node)
        {
            Writter.Keyword("TRY_CONVERT");
            Writter.Text("(");
            node.DataType.Accept(this);
            Writter.Text(",");
            Writter.Space();
            node.Parameter.Accept(this);
            if (node.Style != null)
            {
                Writter.Text(",");
                Writter.Space();
                node.Style.Accept(this);
            }
            Writter.Text(")");

            ExplicitVisit((PrimaryExpression)node);
        }


        override public void ExplicitVisit(BooleanTernaryExpression node)
        {
            node.FirstExpression.Accept(this);

            Writter.Space();
            switch (node.TernaryExpressionType)
            {
                case BooleanTernaryExpressionType.Between:
                    Writter.Keyword("BETWEEN");
                    break;
                case BooleanTernaryExpressionType.NotBetween:
                    Writter.Keyword("NOT");
                    Writter.Space();
                    Writter.Keyword("BETWEEN");
                    break;
            }

            Writter.Space();
            node.SecondExpression.Accept(this);

            Writter.Space();
            Writter.Keyword("AND");

            Writter.Space();
            node.ThirdExpression.Accept(this);
        }

        override public void ExplicitVisit(CommonTableExpression node)
        {
            node.ExpressionName.Accept(this);

            if (node.Columns.Count > 0)
            {
                Writter.Space();
                Writter.Text("(");
                Writter.Join(node.Columns, visitor: this);
                Writter.Text(")");
            }

            Writter.Space();
            Writter.Keyword("AS");

            Writter.Space();
            Writter.Text("(");
            Writter.Indent(() =>
            {
                Writter.NewLine();
                node.QueryExpression.Accept(this);
            });
            Writter.NewLine();
            Writter.Text(")");
        }

        override public void ExplicitVisit(WithCtesAndXmlNamespaces node)
        {
            if (node.CommonTableExpressions.Count > 0)
            {
                Writter.Keyword("WITH");
                Writter.Indent(() =>
                {
                    Writter.NewLine();
                    Writter.Join(node.CommonTableExpressions, visitor: this);
                });
            }
            // public XmlNamespaces XmlNamespaces { get; set; }
            // public ValueExpression ChangeTrackingContext { get; set; }
        }

        override public void ExplicitVisit(StatementWithCtesAndXmlNamespaces node)
        {
            node.WithCtesAndXmlNamespaces?.Accept(this);
            // public IList<OptimizerHint> OptimizerHints { get; }
        }

        override public void ExplicitVisit(BooleanIsNullExpression node)
        {
            node.Expression.Accept(this);

            Writter.Space();
            Writter.Keyword("IS");

            if (node.IsNot)
            {
                Writter.Space();
                Writter.Keyword("NOT");
            }

            Writter.Space();
            Writter.Keyword("NULL");
        }

        override public void ExplicitVisit(ExpressionGroupingSpecification node)
        {
            node.Expression.Accept(this);

            if (node.DistributedAggregation)
            {
                Writter.Keyword("WITH");
                Writter.Space();
                Writter.Text("(");
                Writter.Keyword("DISTRIBUTED_AGG");
                Writter.Text(")");
            }
        }

        override public void ExplicitVisit(GroupByClause node)
        {
            // public GroupByOption GroupByOption { get; set; }

            Writter.Keyword("GROUP");
            Writter.Space();
            Writter.Keyword("BY");
            Writter.Space();

            if (node.All)
            {
                Writter.Keyword("ALL");
                Writter.Space();
            }

            Writter.Join(node.GroupingSpecifications, visitor: this);
        }

        override public void ExplicitVisit(BooleanParenthesisExpression node)
        {
            Writter.Text("(");
            Writter.IndentToCursor(() => node.Expression.Accept(this));
            Writter.Text(")");
        }

        override public void ExplicitVisit(CoalesceExpression node)
        {
            Writter.Keyword("COALESCE");
            Writter.Text("(");
            Writter.Join(node.Expressions, visitor: this);
            Writter.Text(")");
            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(BinaryExpression node)
        {
            node.FirstExpression.Accept(this);
            Writter.Space();
            switch (node.BinaryExpressionType)
            {
                case BinaryExpressionType.Add:
                    Writter.Text("+");
                    break;
                case BinaryExpressionType.Subtract:
                    Writter.Text("-");
                    break;
                case BinaryExpressionType.Multiply:
                    Writter.Text("*");
                    break;
                case BinaryExpressionType.Divide:
                    Writter.Text("/");
                    break;
                case BinaryExpressionType.Modulo:
                    Writter.Text("%");
                    break;
                case BinaryExpressionType.BitwiseAnd:
                    Writter.Text("&");
                    break;
                case BinaryExpressionType.BitwiseOr:
                    Writter.Text("|");
                    break;
                case BinaryExpressionType.BitwiseXor:
                    Writter.Text("^");
                    break;
            }
            Writter.Space();
            node.SecondExpression.Accept(this);
        }

        override public void ExplicitVisit(BinaryQueryExpression node)
        {
            node.FirstQueryExpression.Accept(this);
            Writter.NewLine();
            switch (node.BinaryQueryExpressionType)
            {
                case BinaryQueryExpressionType.Union:
                    Writter.Keyword("UNION");
                    break;
                case BinaryQueryExpressionType.Except:
                    Writter.Keyword("EXCEPT");
                    break;
                case BinaryQueryExpressionType.Intersect:
                    Writter.Keyword("INTERSECT");
                    break;
            }
            if (node.All)
            {
                Writter.Space();
                Writter.Keyword("ALL");
            }
            Writter.NewLine();
            node.SecondQueryExpression.Accept(this);
            ExplicitVisit((QueryExpression)node);
        }

        override public void ExplicitVisit(TableReferenceWithAliasAndColumns node)
        {
            if (node.Columns.Count > 0)
            {
                Writter.Space();
                Writter.Text("(");
                Writter.Join(node.Columns, visitor: this);
                Writter.Text(")");
            }

            ExplicitVisit((TableReferenceWithAlias)node);
        }
        override public void ExplicitVisit(QueryDerivedTable node)
        {
            Writter.Text("(");
            Writter.IndentToCursor(() => node.QueryExpression.Accept(this));
            Writter.Text(")");

            ExplicitVisit((TableReferenceWithAliasAndColumns)node);
        }

        override public void ExplicitVisit(UnqualifiedJoin node)
        {
            node.FirstTableReference.Accept(this);
            Writter.NewLine();

            switch (node.UnqualifiedJoinType)
            {
                case UnqualifiedJoinType.CrossJoin:
                    Writter.Keyword("CROSS");
                    Writter.Space();
                    Writter.Keyword("JOIN");
                    break;
                case UnqualifiedJoinType.CrossApply:
                    Writter.Keyword("CROSS");
                    Writter.Space();
                    Writter.Keyword("APPLY");
                    break;
                case UnqualifiedJoinType.OuterApply:
                    Writter.Keyword("OUTER");
                    Writter.Space();
                    Writter.Keyword("APPLY");
                    break;
            }

            Writter.Space();
            node.SecondTableReference.Accept(this);
        }

        override public void ExplicitVisit(QualifiedJoin node)
        {
            node.FirstTableReference.Accept(this);
            Writter.NewLine();

            switch (node.QualifiedJoinType)
            {
                case QualifiedJoinType.Inner:
                    Writter.Keyword("INNER");
                    break;
                case QualifiedJoinType.LeftOuter:
                    Writter.Keyword("LEFT");
                    break;
                case QualifiedJoinType.RightOuter:
                    Writter.Keyword("RIGHT");
                    break;
                case QualifiedJoinType.FullOuter:
                    Writter.Keyword("FULL");
                    break;
            }

            switch (node.JoinHint)
            {
                case JoinHint.Loop:
                    Writter.Space();
                    Writter.Keyword("LOOP");
                    break;
                case JoinHint.Hash:
                    Writter.Space();
                    Writter.Keyword("HASH");
                    break;
                case JoinHint.Merge:
                    Writter.Space();
                    Writter.Keyword("MERGE");
                    break;
                case JoinHint.Remote:
                    Writter.Space();
                    Writter.Keyword("REMOTE");
                    break;
            }

            Writter.Space();
            Writter.Keyword("JOIN");

            Writter.Space();
            node.SecondTableReference.Accept(this);

            if (node.SearchCondition != null)
            {
                Writter.Space();
                Writter.Keyword("ON");
                Writter.Space();
                Writter.IndentToCursor(() => node.SearchCondition.Accept(this));
            }
        }

        override public void ExplicitVisit(FunctionCall node)
        {
            if (node.CallTarget != null)
            {
                node.CallTarget.Accept(this);
                Writter.Text(".");
            }

            node.FunctionName.Accept(this);

            Writter.Text("(");
            switch (node.UniqueRowFilter)
            {
                case UniqueRowFilter.All:
                    Writter.Keyword("ALL");
                    Writter.Space();
                    break;
                case UniqueRowFilter.Distinct:
                    Writter.Keyword("DISTINCT");
                    Writter.Space();
                    break;
            }
            Writter.Join(node.Parameters, visitor: this);
            Writter.Text(")");

            if (node.OverClause != null)
            {
                Writter.Space();
                node.OverClause.Accept(this);
            }

            if (node.WithinGroupClause != null)
            {
                Writter.Space();
                node.WithinGroupClause.Accept(this);
            }

            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(IdentifierOrValueExpression node)
        {
            node.Identifier?.Accept(this);
            node.ValueExpression?.Accept(this);
        }

        override public void ExplicitVisit(SelectScalarExpression node)
        {
            node.Expression.Accept(this);
            if (node.ColumnName != null)
            {
                Writter.Space();
                node.ColumnName.Accept(this);
            }
        }

        override public void ExplicitVisit(LikePredicate node)
        {
            node.FirstExpression.Accept(this);

            if (node.NotDefined)
            {
                Writter.Space();
                Writter.Keyword("NOT");
            }
            Writter.Space();
            Writter.Keyword("LIKE");

            Writter.Space();
            node.SecondExpression.Accept(this);

            if (node.EscapeExpression != null)
            {
                Writter.Space();
                Writter.Keyword("ESCAPE");
                Writter.Space();
                node.EscapeExpression.Accept(this);
            }
            // public bool OdbcEscape { get; set; }
        }

        override public void ExplicitVisit(BooleanComparisonExpression node)
        {
            node.FirstExpression.Accept(this);
            Writter.Space();
            switch (node.ComparisonType)
            {
                case BooleanComparisonType.Equals:
                    Writter.Text("=");
                    break;
                case BooleanComparisonType.GreaterThan:
                    Writter.Text(">");
                    break;
                case BooleanComparisonType.LessThan:
                    Writter.Text("<");
                    break;
                case BooleanComparisonType.GreaterThanOrEqualTo:
                    Writter.Text(">=");
                    break;
                case BooleanComparisonType.LessThanOrEqualTo:
                    Writter.Text("<=");
                    break;
                case BooleanComparisonType.NotEqualToBrackets:
                    Writter.Text("<>");
                    break;
                case BooleanComparisonType.NotEqualToExclamation:
                    Writter.Text("!=");
                    break;
                case BooleanComparisonType.NotLessThan:
                    Writter.Text("!<");
                    break;
                case BooleanComparisonType.NotGreaterThan:
                    Writter.Text("!>");
                    break;
                case BooleanComparisonType.LeftOuterJoin:
                    Writter.Text("*=");
                    break;
                case BooleanComparisonType.RightOuterJoin:
                    Writter.Text("=*");
                    break;
            }
            Writter.Space();
            node.SecondExpression.Accept(this);
        }

        override public void ExplicitVisit(BooleanBinaryExpression node)
        {
            node.FirstExpression.Accept(this);
            Writter.NewLine();
            switch (node.BinaryExpressionType)
            {
                case BooleanBinaryExpressionType.And:
                    Writter.Keyword("AND");
                    break;
                case BooleanBinaryExpressionType.Or:
                    Writter.Keyword("OR");
                    break;
            }
            Writter.Space();
            node.SecondExpression.Accept(this);
        }

        override public void ExplicitVisit(WhereClause node)
        {
            Writter.Keyword("WHERE");
            Writter.Space();
            Writter.IndentToCursor(() => node.SearchCondition.Accept(this));
            // public CursorId Cursor { get; set; }
        }

        override public void ExplicitVisit(TableReferenceWithAlias node)
        {
            if (node.Alias != null)
            {
                Writter.Space();
                node.Alias.Accept(this);
            }
        }

        override public void ExplicitVisit(SchemaObjectName node)
        {
            ExplicitVisit((MultiPartIdentifier)node);
        }

        override public void ExplicitVisit(NamedTableReference node)
        {
            node.SchemaObject.Accept(this);

            if (node.TableHints.Count > 0)
            {
                Writter.Space();
                Writter.Keyword("WITH");
                Writter.Text("(");
                Writter.Join(node.TableHints, visitor: this);
                Writter.Text(")");
            }
            // public TableSampleClause TableSampleClause { get; set; }
            // public TemporalClause TemporalClause { get; set; }
            // public bool ForPath { get; set; }
            ExplicitVisit((TableReferenceWithAlias)node);
        }

        override public void ExplicitVisit(FromClause node)
        {
            Writter.Keyword("FROM");
            Writter.Space();

            Writter.Join(separator: _ => { Writter.Text(","); Writter.NewLine(); }, node.TableReferences, visitor: this);
        }

        override public void ExplicitVisit(SelectStarExpression node)
        {
            if (node.Qualifier != null)
            {
                node.Qualifier.Accept(this);
                Writter.Text(".");
            }

            Writter.Text("*");
        }

        override public void ExplicitVisit(Literal node)
        {
            Writter.Text(node.Value);
        }

        override public void ExplicitVisit(IntegerLiteral node)
        {
            ExplicitVisit((Literal)node);
        }

        override public void ExplicitVisit(NumericLiteral node)
        {
            ExplicitVisit((Literal)node);
        }

        override public void ExplicitVisit(NullLiteral node)
        {
            Writter.Keyword("NULL");
        }

        override public void ExplicitVisit(StringLiteral node)
        {
            Writter.Text("'" + node.Value.Replace(oldValue: "'", newValue: "''") + "'");
        }

        override public void ExplicitVisit(TopRowFilter node)
        {
            Writter.Keyword("TOP");
            Writter.Space();
            node.Expression.Accept(this);

            if (node.Percent)
            {
                Writter.Space();
                Writter.Keyword("PERCENT");
            }

            if (node.WithTies)
            {
                Writter.Space();
                Writter.Keyword("WITH");
                Writter.Space();
                Writter.Keyword("TIES");
            }
        }

        override public void ExplicitVisit(Identifier node)
        {
            Writter.Text(Identifier.EncodeIdentifier(node.Value, node.QuoteType));
        }

        override public void ExplicitVisit(MultiPartIdentifier node)
        {
            Writter.Join(separator: _ => { Writter.Text("."); }, node.Identifiers, visitor: this);
        }

        override public void ExplicitVisit(PrimaryExpression node)
        {
            if (node.Collation != null)
            {
                Writter.Space();
                Writter.Keyword("COLLATE");
                Writter.Space();
                node.Collation.Accept(this);
            }
        }

        override public void ExplicitVisit(ColumnReferenceExpression node)
        {
            // public ColumnType ColumnType { get; set; }
            switch (node.ColumnType)
            {
                case ColumnType.Regular:
                    node.MultiPartIdentifier.Accept(this);
                    break;
                case ColumnType.Wildcard:
                    Writter.Text("*");
                    break;
                default:
                    Visit((TSqlFragment)node);
                    break;
            }
            ExplicitVisit((PrimaryExpression)node);
        }

        override public void ExplicitVisit(ExpressionWithSortOrder node)
        {
            node.Expression.Accept(this);
            switch (node.SortOrder)
            {
                // case SortOrder.Ascending:
                //     Writter.Space();
                //     Writter.Text("ASC");
                //     break;
                case SortOrder.Descending:
                    Writter.Space();
                    Writter.Keyword("DESC");
                    break;
            }
        }

        override public void ExplicitVisit(OrderByClause node)
        {
            Writter.Keyword("ORDER");
            Writter.Space();
            Writter.Keyword("BY");
            Writter.Space();
            Writter.Join(node.OrderByElements, visitor: this);
        }

        public override void ExplicitVisit(HavingClause node)
        {
            Writter.Keyword("HAVING");
            Writter.Space();
            Writter.IndentToCursor(() => node.SearchCondition.Accept(this));
        }

        override public void ExplicitVisit(QueryExpression node)
        {
            if (node.OrderByClause != null)
            {
                Writter.NewLine();
                node.OrderByClause.Accept(this);
            }

            if (node.OffsetClause != null)
            {
                Writter.NewLine();
                node.OffsetClause.Accept(this);
            }

            if (node.ForClause != null)
            {
                Writter.NewLine();
                node.ForClause.Accept(this);
            }
        }

        override public void ExplicitVisit(QuerySpecification node)
        {
            Writter.Keyword("SELECT");
            Writter.Space();

            switch (node.UniqueRowFilter)
            {
                case UniqueRowFilter.All:
                    Writter.Keyword("ALL");
                    Writter.Space();
                    break;
                case UniqueRowFilter.Distinct:
                    Writter.Keyword("DISTINCT");
                    Writter.Space();
                    break;
            }

            if (node.TopRowFilter != null)
            {
                node.TopRowFilter.Accept(this);
                Writter.Space();
            }

            Writter.Join(node.SelectElements, visitor: this);

            if (node.FromClause != null)
            {
                Writter.NewLine();
                node.FromClause.Accept(this);
            }

            if (node.WhereClause != null)
            {
                Writter.NewLine();
                node.WhereClause.Accept(this);
            }

            if (node.GroupByClause != null)
            {
                Writter.NewLine();
                node.GroupByClause.Accept(this);
            }

            if (node.HavingClause != null)
            {
                Writter.NewLine();
                node.HavingClause.Accept(this);
            }

            ExplicitVisit((QueryExpression)node);
        }

        override public void ExplicitVisit(SelectStatement node)
        {
            if (node.WithCtesAndXmlNamespaces != null)
            {
                ExplicitVisit((StatementWithCtesAndXmlNamespaces)node);
                Writter.NewLine();
            }

            node.QueryExpression.Accept(this);
            // public SchemaObjectName Into { get; set; }
            // public Identifier On { get; set; }
            // public IList<ComputeClause> ComputeClauses { get; }
        }

        override public void ExplicitVisit(TSqlBatch node)
        {
            Writter.Join(separator: _ => { Writter.NewLine(); Writter.NewLine(); }, node.Statements, visitor: this);
        }

        override public void ExplicitVisit(TSqlScript node)
        {
            Writter.Join(separator: _ => { Writter.NewLine(); Writter.NewLine(); }, node.Batches, visitor: this);
        }
    }
}