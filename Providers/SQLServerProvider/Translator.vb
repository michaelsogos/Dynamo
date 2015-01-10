Imports System.Text
Imports System.Linq.Expressions
Imports Dynamo
Imports Dynamo.Entities

Namespace Translators
    Friend Class TSQLTranslator
        Inherits Expressions.ExpressionVisitor

        Private TempBuilder As StringBuilder
        Private QueryBuilder As StringBuilder
        Private WhereQueryBuilder As StringBuilder
        Private OrderQueryBuilder As StringBuilder
        Private JoinQueryBuilder As StringBuilder
        Private EntitiesAlias As Dictionary(Of String, String)
        Private IsFirstVisit As Boolean

        Public Function Translate(ByVal expression As Expression) As String
            EntitiesAlias = New Dictionary(Of String, String)
            TempBuilder = New StringBuilder()
            QueryBuilder = New StringBuilder("SELECT |*| FROM")
            WhereQueryBuilder = New StringBuilder()
            OrderQueryBuilder = New StringBuilder()
            JoinQueryBuilder = New StringBuilder()
            IsFirstVisit = True
            Visit(expression)
            Return QueryBuilder.Append(JoinQueryBuilder).Append(WhereQueryBuilder).Append(OrderQueryBuilder).ToString
        End Function

        Public Overrides Function Visit(node As Expression) As Expression

            Select Case node.NodeType
                Case ExpressionType.Call
                    Visit(DirectCast(node, MethodCallExpression).Arguments(0))
                    Return VisitMethodCall(node)
                Case ExpressionType.Constant
                    Return node
                Case ExpressionType.Lambda
                    Dim body As Expression = DirectCast(node, LambdaExpression).Body
                    Select Case body.NodeType
                        Case ExpressionType.Call
                            Return body
                        Case ExpressionType.Convert, ExpressionType.Equal
                            Return Visit(body)
                        Case Else
                            Throw New NotImplementedException("Lambda " + body.NodeType.ToString)
                    End Select
                Case ExpressionType.MemberAccess
                    Return DirectCast(node, MemberExpression).Expression
                Case ExpressionType.Quote
                    Return Visit(DirectCast(node, UnaryExpression).Operand)
                Case ExpressionType.Convert
                    Return Visit(DirectCast(node, UnaryExpression).Operand)
                Case ExpressionType.And, ExpressionType.AndAlso, ExpressionType.Or, ExpressionType.OrElse, ExpressionType.Equal
                    Return VisitBinary(node)
                Case Else
                    Throw New NotImplementedException(node.NodeType.ToString)
            End Select

        End Function

        Private Function IsNullConstant(expression As Expression) As Boolean
            Return (expression.NodeType = ExpressionType.Constant AndAlso DirectCast(expression, ConstantExpression).Value Is Nothing)
        End Function

        Protected Overrides Function VisitMethodCall(node As MethodCallExpression) As Expression
            Select Case node.Method.Name.ToLower
                Case "query"
                    Dim EntityName As ConstantExpression = node.Arguments(0)
                    If IsFirstVisit Then
                        TempBuilder.Clear()
                        ElaborateArgument(EntityName, "[{0}]")
                        QueryBuilder.AppendFormat(" {0} ", TempBuilder.ToString)
                        IsFirstVisit = False
                        If Not EntitiesAlias.ContainsValue(EntityName.Value) Then EntitiesAlias.Add("", EntityName.Value)
                    End If
                    Return node.Arguments(0)
                Case "select"
                    Visit(node.Arguments(0))
                Case "where"
                    If WhereQueryBuilder.Length <= 0 Then WhereQueryBuilder.Append(" WHERE |*|")
                    TempBuilder.Clear()
                    Visit(node.Arguments(1))
                    WhereQueryBuilder.Replace("|*|", TempBuilder.ToString)
                Case "join"
                    TempBuilder.Clear()

                    ElaborateArgument(Visit(node.Arguments(1)), "INNER JOIN [{0}] ON [{0}].")
                    ElaborateArgument(Visit(Visit(node.Arguments(3))), "[{0}] = ")
                    ElaborateArgument(Visit(node.Arguments(0)), "[{0}].")
                    ElaborateArgument(Visit(Visit(node.Arguments(2))), "[{0}] ")

                    JoinQueryBuilder.Append(TempBuilder.ToString)
                Case "orderby"
                    If OrderQueryBuilder.Length <= 0 Then OrderQueryBuilder.Append(" ORDER BY |*|")
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Append(" ASC").ToString)
                Case "orderbydescending"
                    If OrderQueryBuilder.Length <= 0 Then OrderQueryBuilder.Append(" ORDER BY |*|")
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Append(" DESC").ToString)
                Case "thenby"
                    If OrderQueryBuilder.Length <= 0 Then OrderQueryBuilder.Append(" ORDER BY |*|")
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Insert(0, "|*|,").Append(" ASC").ToString)
                Case "thenbydescending"
                    If OrderQueryBuilder.Length <= 0 Then OrderQueryBuilder.Append(" ORDER BY |*|")
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Insert(0, "|*|,").Append(" DESC").ToString)
                Case "get_item"
                    If TypeOf node.Object Is MemberExpression AndAlso DirectCast(node.Object, MemberExpression).Member.Name.ToLower = "fields" Then
                        Return node.Arguments(0)
                    End If
                Case "compareobjectequal"
                    ElaborateArgument(node.Arguments(0))
                    TempBuilder.Append(" = ")
                    ElaborateArgument(node.Arguments(1))
                Case "compareobjectnotequal"
                    ElaborateArgument(node.Arguments(0))
                    TempBuilder.Append(" <> ")
                    ElaborateArgument(node.Arguments(1))
                Case "comparestring"
                    ElaborateArgument(node.Arguments(0), "'{0}'")
                    TempBuilder.Append(" |*| ")
                    ElaborateArgument(node.Arguments(1), "'{0}'")
                Case Else
                    Throw New NotImplementedException()
            End Select

        End Function

        Private Sub ElaborateArgument(ByRef Argument As expression, Optional ByVal Pattern As String = Nothing)
            Select Case Argument.NodeType
                Case ExpressionType.Constant
                    Dim Value
                    Select Case Argument.Type
                        Case GetType(Boolean)
                            Value = If(DirectCast(Argument, ConstantExpression).Value, 1, 0)
                        Case Else
                            Value = DirectCast(Argument, ConstantExpression).Value
                    End Select
                    If String.IsNullOrWhiteSpace(Pattern) Then
                        TempBuilder.Append(Value)
                    Else
                        TempBuilder.AppendFormat(Pattern, Value)
                    End If
                Case ExpressionType.Call
                    TempBuilder.AppendFormat("[{0}].[{1}]", GetFieldAliasName(Argument), DirectCast(Me.Visit(Argument), ConstantExpression).Value)
                Case ExpressionType.Convert
                    ElaborateArgument(Me.Visit(DirectCast(Argument, UnaryExpression).Operand))
                Case ExpressionType.Lambda
                    ElaborateArgument(DirectCast(Argument, LambdaExpression).Body)
                Case ExpressionType.Quote
                    ElaborateArgument(DirectCast(Argument, UnaryExpression).Operand)
                Case ExpressionType.MemberAccess
                    TempBuilder.AppendFormat("[{0}].[{1}]", GetFieldAliasName(Argument), DirectCast(Argument, MemberExpression).Member.Name)
                Case Else
                    ElaborateArgument(Me.Visit(Argument))
            End Select
        End Sub

        Private Function GetFieldAliasName(ByRef Argument As expression) As String
            Select Case Argument.NodeType
                Case ExpressionType.Call
                    Dim ArgumentObject = DirectCast(Argument, MethodCallExpression).Object
                    Return If(ArgumentObject IsNot Nothing, GetFieldAliasName(DirectCast(ArgumentObject, MemberExpression).Expression), Nothing)
                Case ExpressionType.MemberAccess
                    Dim ArgumentExpression = DirectCast(Argument, MemberExpression).Expression
                    Select Case ArgumentExpression.NodeType
                        Case ExpressionType.MemberAccess
                            Return GetFieldAliasName(ArgumentExpression)
                        Case ExpressionType.Parameter
                            If Not EntitiesAlias.ContainsKey(DirectCast(ArgumentExpression, ParameterExpression).Name) Then
                                EntitiesAlias.Add(DirectCast(ArgumentExpression, ParameterExpression).Name, EntitiesAlias(""))
                                EntitiesAlias.Remove("")
                            End If
                            Return EntitiesAlias(DirectCast(ArgumentExpression, ParameterExpression).Name)
                            'Return DirectCast(ArgumentExpression, ParameterExpression).Name
                        Case Else
                            Throw New NotImplementedException("Field alias name not retrievable for " + ArgumentExpression.NodeType.ToString)
                    End Select
            End Select

            Return Nothing
        End Function

        Protected Overrides Function VisitBinary(node As BinaryExpression) As Expression
            Dim Left = Me.Visit(node.Left)

            Select Case node.NodeType
                Case ExpressionType.And, ExpressionType.AndAlso
                    TempBuilder.Append(" AND ")
                Case ExpressionType.Or, ExpressionType.OrElse
                    TempBuilder.Append(" OR ")
                Case ExpressionType.Equal
                    If IsNullConstant(node.Right) Then
                        ElaborateArgument(node.Left)
                        TempBuilder.Append(" IS NULL")
                    Else
                        TempBuilder.Replace("|*|", "=")
                        Return node
                    End If
                Case Else
                    Throw New NotImplementedException("Binary operator " + node.NodeType.ToString)
            End Select

            Dim Right = Me.Visit(node.Right)
            If TypeOf Right Is ConstantExpression Then TempBuilder.Append(DirectCast(Right, ConstantExpression).Value)

            Return node
        End Function

        Protected Overrides Function VisitConstant(node As ConstantExpression) As Expression
            If node.Value IsNot Nothing Then
                Select Case node.Value.GetType
                    Case GetType(DynamoQueryable(Of Entity))
                        Dim a = 0
                End Select
            End If
            Return node
        End Function

    End Class
End Namespace