Imports System.Text
Imports System.Linq.Expressions
Imports Dynamo

Namespace Translators
    Friend Class TSQLTranslator
        Inherits Expressions.ExpressionVisitor

        Private SQLQuery As StringBuilder

        Public Function Translate(ByVal expression As Expression)
            SQLQuery = New StringBuilder
            Visit(expression)
            Return SQLQuery.ToString()
        End Function

        Protected Overrides Function VisitMethodCall(node As MethodCallExpression) As Expression
            Return MyBase.VisitMethodCall(node)
        End Function

        Protected Overrides Function VisitBinary(node As BinaryExpression) As Expression
            Return MyBase.VisitBinary(node)
        End Function

        Protected Overrides Function VisitConstant(node As ConstantExpression) As Expression
            Select Case node.Value.GetType
                Case GetType(DynamoQueryable)
                    Dim NodeValue As DynamoQueryable = node.Value
                    If (NodeValue IsNot Nothing AndAlso Not String.IsNullOrWhiteSpace(NodeValue.EntityName)) Then
                        SQLQuery.AppendFormat(" SELECT * FROM {0} ", NodeValue.EntityName)
                    Else
                        SQLQuery.Append(" NULL ")
                    End If
                Case Else
                    Throw New NotSupportedException("The type " + node.Value.GetType.Name + " is not supported!")
            End Select
            Return node
        End Function

        Protected Overrides Function VisitMember(node As MemberExpression) As Expression
            Return MyBase.VisitMember(node)
        End Function

    End Class
End Namespace