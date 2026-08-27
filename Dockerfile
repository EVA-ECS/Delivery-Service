FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Contracts/EVA-ECS.Chat.Contracts/EVA-ECS.Chat.Contracts.csproj Contracts/EVA-ECS.Chat.Contracts/
COPY Delivery-Service/Delivery-Service.csproj Delivery-Service/
RUN dotnet restore Delivery-Service/Delivery-Service.csproj --source https://api.nuget.org/v3/index.json

COPY Contracts/EVA-ECS.Chat.Contracts/ Contracts/EVA-ECS.Chat.Contracts/
COPY Delivery-Service/ Delivery-Service/
RUN dotnet publish Delivery-Service/Delivery-Service.csproj -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Delivery-Service.dll"]
