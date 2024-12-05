from django import forms
from django.contrib.auth.forms import UserCreationForm, UserChangeForm, AuthenticationForm
from django.core.validators import RegexValidator
from .models import CustomUser


class CustomUserCreationForm(UserCreationForm):
    """
    Form for creating a new user in the Air Quality Monitoring System
    """
    organization = forms.CharField(
        max_length=100,
        required=False,
        help_text="Optional: Organization you belong to"
    )
    phone_number = forms.CharField(
        max_length=15,
        required=False,
        validators=[
            RegexValidator(
                regex=r'^\+?1?\d{9,15}$',
                message="Phone number must be entered in the format: '+999999999'. Up to 15 digits allowed."
            )
        ],
        help_text="Optional: Your contact phone number"
    )
    email = forms.EmailField(
        required=True,
        help_text="Required. Enter a valid email address"
    )

    class Meta:
        model = CustomUser
        fields = ('username', 'email', 'password1', 'password2', 'organization', 'phone_number')

    def save(self, commit=True):
        """
        Save the user with additional custom fields
        """
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        user.organization = self.cleaned_data.get('organization', '')
        user.phone_number = self.cleaned_data.get('phone_number', '')

        if commit:
            user.save()
        return user


class CustomUserChangeForm(UserChangeForm):
    """
    Form for updating user information
    """

    class Meta:
        model = CustomUser
        fields = ('username', 'email', 'organization', 'phone_number')


class CustomAuthenticationForm(AuthenticationForm):
    """
    Custom authentication form with additional styling and validation
    """
    username = forms.CharField(
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Username'
        })
    )
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Password'
        })
    )

    class Meta:
        model = CustomUser
        fields = ('username', 'password')