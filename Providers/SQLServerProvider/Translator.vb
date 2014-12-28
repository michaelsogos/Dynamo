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

        Public Function Translate(ByVal expression As Expression) As String
            TempBuilder = New StringBuilder()
            QueryBuilder = New StringBuilder("SELECT")
            WhereQueryBuilder = New StringBuilder(" WHERE |*|")
            OrderQueryBuilder = New StringBuilder(" ORDER BY |*|")
            Visit(expression)
            Return QueryBuilder.Append(WhereQueryBuilder).Append(OrderQueryBuilder).ToString
        End Function

        'Private Function StripQuote(expression As Expression) As Expression
        '    While (expression.NodeType = ExpressionType.Quote)
        '        expression = DirectCast(expression, UnaryExpression).Operand
        '    End While
        '    Return expression
        'End Function

        Private Function IsNullConstant(expression As Expression) As Boolean
            Return (expression.NodeType = ExpressionType.Constant AndAlso DirectCast(expression, ConstantExpression).Value Is Nothing)
        End Function

        Protected Overrides Function VisitMethodCall(node As MethodCallExpression) As Expression
            Select Case node.Method.Name.ToLower
                Case "where"
                    TempBuilder.Clear()
                    Me.Visit(DirectCast(node.Arguments(1), UnaryExpression).Operand)
                    WhereQueryBuilder.Replace("|*|", TempBuilder.ToString)
                Case "join"
                    TempBuilder.Clear()
                    Dim Left As LambdaExpression = Me.Visit(DirectCast(node.Arguments(2), UnaryExpression).Operand)
                    Dim RightEntityName = DirectCast(DirectCast(node.Arguments(1), ConstantExpression).Value, DynamoQueryable(Of Entity)).EntityName
                    Dim Right As LambdaExpression = Me.Visit(DirectCast(node.Arguments(3), UnaryExpression).Operand)
                    TempBuilder.AppendFormat("INNER JOIN [{0}] AS [{1}] ON [{1}].[{2}] = [{3}].[{4}]", RightEntityName, Right.Parameters(0), DirectCast(Right.Body, ConstantExpression).Value, Left.Parameters(0), DirectCast(Left.Body, ConstantExpression).Value)
                    QueryBuilder.Append(TempBuilder)
                Case "orderby"
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Append(" ASC").ToString)
                Case "orderbydescending"
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Append(" DESC").ToString)
                Case "thenby"
                    TempBuilder.Clear()
                    ElaborateArgument(node.Arguments(1))
                    OrderQueryBuilder.Replace("|*|", TempBuilder.Insert(0, "|*|,").Append(" ASC").ToString)
                Case "thenbydescending"
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
                Case Else
                    Throw New NotImplementedException()
            End Select
            Return MyBase.VisitMethodCall(node)
        End Function

        Private Sub ElaborateArgument(ByRef Argument As expression)
            Select Case Argument.NodeType
                Case ExpressionType.Constant
                    TempBuilder.AppendFormat("{0}", DirectCast(Argument, ConstantExpression).Value)
                Case ExpressionType.Call
                    TempBuilder.AppendFormat("[{0}].[{1}]", GetFieldAliasName(Argument), DirectCast(Me.Visit(Argument), ConstantExpression).Value)
                Case ExpressionType.Convert
                    ElaborateArgument(Me.Visit(DirectCast(Argument, UnaryExpression).Operand))
                Case ExpressionType.Lambda
                    ElaborateArgument(DirectCast(Argument, LambdaExpression).Body)
                Case ExpressionType.Quote
                    ElaborateArgument(DirectCast(Argument, UnaryExpression).Operand)
                Case Else
                    ElaborateArgument(Me.Visit(Argument))
            End Select
        End Sub

        Private Function GetFieldAliasName(ByRef Argument As expression) As String
            Select Case Argument.NodeType
                Case ExpressionType.Call
                    Return GetFieldAliasName(DirectCast(DirectCast(Argument, MethodCallExpression).Object, MemberExpression).Expression)
                Case ExpressionType.MemberAccess
                    Return DirectCast(Argument, MemberExpression).Member.Name
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
                        TempBuilder.Append(" = ")
                    End If
            End Select

            Dim Right = Me.Visit(node.Right)
            If TypeOf Right Is ConstantExpression Then TempBuilder.Append(DirectCast(Right, ConstantExpression).Value)

            Return node
        End Function

        Protected Overrides Function VisitUnary(node As UnaryExpression) As Expression
            'Select Case node.NodeType
            '    Case ExpressionType.Not
            '        SQLQuery.Append(" NOT ")
            '        Me.Visit(node.Operand)
            '    Case ExpressionType.Convert
            '        Me.Visit(node.Operand)
            '    Case Else
            '        Dim a = 0
            'End Select
            Return MyBase.VisitUnary(node)
        End Function

        Protected Overrides Function VisitConstant(node As ConstantExpression) As Expression
            'Select Case node.Value.GetType
            '    Case GetType(DynamoQueryable)
            '        Dim NodeValue As DynamoQueryable = node.Value
            '        If (NodeValue IsNot Nothing AndAlso Not String.IsNullOrWhiteSpace(NodeValue.EntityName)) Then
            '            SQLQuery.AppendFormat(" SELECT * FROM {0} AS ", NodeValue.EntityName)
            '        Else
            '            SQLQuery.Append(" NULL ")
            '        End If
            '    Case Else
            '        'Throw New NotSupportedException("The type " + node.Value.GetType.Name + " is not supported!")
            'End Select
            Return node
        End Function

        Protected Overrides Function VisitMember(node As MemberExpression) As Expression
            'If node.Expression IsNot Nothing AndAlso node.Expression.NodeType = ExpressionType.Parameter Then
            '    SQLQuery.Append(node.Member.Name)
            'End If
            Return MyBase.VisitMember(node)
        End Function



    End Class
End Namespace