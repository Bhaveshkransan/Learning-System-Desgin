# Interactive Test Suite for Transactional Outbox + RabbitMQ
param (
    [string]$Test = "1"
)

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "   SYSTEM DESIGN MASTERCLASS: TEST RUNNER" -ForegroundColor Cyan
Write-Host "========================================================`n" -ForegroundColor Cyan

function Print-Section($title) {
    Write-Host "`n---> $title" -ForegroundColor Yellow
}

function Show-System-State($orderId) {
    Print-Section "Checking PostgreSQL: orders table"
    docker exec postgres-demo psql -U postgres -d orders_db -c "SELECT id, customer, product, quantity, status, updated_at FROM orders WHERE id = '$orderId';"

    Print-Section "Checking PostgreSQL: outbox_events table"
    docker exec postgres-demo psql -U postgres -d orders_db -c "SELECT id, aggregate_id, event_type, status, created_at, published_at FROM outbox_events WHERE aggregate_id = '$orderId';"

    Print-Section "Checking RabbitMQ queue depth"
    docker exec rabbitmq-demo rabbitmqctl list_queues name messages_ready messages_unacknowledged
}

if ($Test -eq "health") {
    Print-Section "Verifying Infrastructure Health"
    docker ps --filter "name=postgres-demo" --filter "name=rabbitmq-demo"
    Write-Host "`nTesting API at http://localhost:3000/orders..." -ForegroundColor Green
    Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method GET
    exit 0
}

if ($Test -eq "1") {
    Write-Host "[TEST 1: NORMAL ORDER SUBMISSION]" -ForegroundColor Green
    Write-Host "Action: Submitting order via POST http://localhost:3000/orders..." -ForegroundColor Gray
    
    $payload = @{
        customer = "Bhavesh"
        product = "4K OLED Monitor"
        quantity = 1
    } | ConvertTo-Json

    $response = Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method POST -ContentType "application/json" -Body $payload
    $orderId = $response.order.id
    $outboxId = $response.outboxEvent.id

    Write-Host "`n[API RESPONSE - 202 ACCEPTED]" -ForegroundColor Green
    Write-Host "  Order ID       : $orderId" -ForegroundColor White
    Write-Host "  Outbox Event ID: $outboxId" -ForegroundColor White

    Write-Host "`nWaiting 2.5 seconds for Outbox Publisher and Consumer to finish..." -ForegroundColor Gray
    Start-Sleep -Seconds 3

    Show-System-State -orderId $orderId

    Write-Host "`n[TEST 1 RESULT] Completed! Inspect the tables above." -ForegroundColor Green
}
