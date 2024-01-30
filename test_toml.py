import toml

with open('pyproject.toml', 'r') as file:
    data = toml.load(file)
    print(data)