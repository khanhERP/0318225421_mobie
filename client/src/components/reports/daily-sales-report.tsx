import React, { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  Calendar,
  DollarSign,
  ChevronRight,
  Filter,
  Receipt,
} from "lucide-react";
import { useTranslation } from "@/lib/i18n";
import { format, subDays, startOfMonth, endOfMonth } from "date-fns";
import { vi } from "date-fns/locale";
import logoPath from "@assets/EDPOS_1753091767028.png";

interface OrderData {
  id: number;
  orderNumber: string;
  status: string;
  total: string;
  subtotal: string;
  paymentMethod: string;
  orderedAt: string;
  customerId?: number;
  tableId?: number;
  customerCount?: number;
}

interface DailySalesData {
  date: string;
  revenue: number;
  orders: OrderData[];
  orderCount: number;
}

interface DailySalesReportProps {
  onBack?: () => void;
}

export function DailySalesReport({ onBack }: DailySalesReportProps) {
  const { t } = useTranslation();
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [selectedOrder, setSelectedOrder] = useState<OrderData | null>(null);
  const [dateRange, setDateRange] = useState(() => {
    const today = new Date();
    const start = startOfMonth(today);
    const end = endOfMonth(today);
    return {
      start: format(start, "yyyy-MM-dd"),
      end: format(end, "yyyy-MM-dd"),
    };
  });
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [activeFilter, setActiveFilter] = useState("thisMonth"); // Track active filter button

  // Fetch orders for the date range
  const { data: orders = [], isLoading } = useQuery({
    queryKey: ["daily-sales-orders", dateRange.start, dateRange.end],
    queryFn: async () => {
      const response = await fetch(
        `https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/orders/date-range/${dateRange.start}/${dateRange.end}`,
      );
      if (!response.ok) {
        throw new Error("Failed to fetch orders");
      }
      return response.json() as Promise<OrderData[]>;
    },
  });

  let getPaymentMethodName = (method: string): string => {
    const names = {
      cash: t("common.cash"),
      creditCard: t("common.creditCard"),
      debitCard: t("common.debitCard"),
      card: t("common.transfer"),
      momo: t("common.momo"),
      zalopay: t("common.zalopay"),
      vnpay: t("common.vnpay"),
      qrCode: t("common.qrCode"),
      shopeepay: t("common.shopeepay"),
      grabpay: t("common.grabpay"),
    };
    return names[method as keyof typeof names] || t("common.cash");
  };

  // Fetch order items for selected order
  const { data: selectedOrderItems = [] } = useQuery({
    queryKey: ["order-items", selectedOrder?.id],
    queryFn: async () => {
      if (!selectedOrder?.id) return [];
      const response = await fetch(`https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/order-items/${selectedOrder.id}`);
      if (!response.ok) {
        throw new Error("Failed to fetch order items");
      }
      return response.json();
    },
    enabled: !!selectedOrder?.id,
  });

  // Group orders by date
  const dailySalesData: DailySalesData[] = useMemo(() => {
    const groupedData: { [date: string]: DailySalesData } = {};

    // Filter only paid/completed orders
    const paidOrders = orders.filter(
      (order) => order.status === "paid" || order.status === "completed",
    );

    paidOrders.forEach((order) => {
      const orderDate = new Date(order.orderedAt);
      const dateKey = format(orderDate, "yyyy-MM-dd");

      if (!groupedData[dateKey]) {
        groupedData[dateKey] = {
          date: dateKey,
          revenue: 0,
          orders: [],
          orderCount: 0,
        };
      }

      groupedData[dateKey].revenue += parseFloat(order.total || "0");
      groupedData[dateKey].orders.push(order);
      groupedData[dateKey].orderCount += 1;
    });

    // Convert to array and sort by date (newest first)
    return Object.values(groupedData).sort((a, b) =>
      b.date.localeCompare(a.date),
    );
  }, [orders]);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat("vi-VN").format(amount);
  };

  const formatDate = (dateStr: string) => {
    try {
      const date = new Date(dateStr);
      return format(date, "dd/MM/yyyy", { locale: vi });
    } catch {
      return dateStr;
    }
  };

  const formatOrderTime = (dateStr: string) => {
    try {
      const date = new Date(dateStr);
      return format(date, "dd/MM/yyyy HH:mm", { locale: vi });
    } catch {
      return dateStr;
    }
  };

  const getStatusText = (status: string) => {
    const statusMap = {
      paid: "Đã thanh toán",
      completed: "Hoàn thành",
      pending: "Chờ xử lý",
      preparing: "Đang chuẩn bị",
      served: "Đã phục vụ",
      cancelled: "Đã hủy",
    };
    return statusMap[status as keyof typeof statusMap] || status;
  };

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="text-lg text-gray-500">{t("common.loading")}</div>
      </div>
    );
  }

  // Show order receipt view
  if (selectedOrder) {
    console.log("Selected order items:", selectedOrderItems);
    console.log("Selected order details:", selectedOrder);

    return (
      <div className="min-h-screen bg-green-50">
        {/* Header */}
        <div className="bg-green-600 text-white p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              className="text-white hover:bg-green-700"
              onClick={() => setSelectedOrder(null)}
            >
              <ArrowLeft className="w-5 h-5" />
            </Button>
            <h1 className="text-lg font-semibold">
              {t("orders.orderDetails")}
            </h1>
          </div>
          <div className="flex items-center">
            <img
              src={logoPath}
              alt="EDPOS Logo"
              className="h-8 md:h-12 object-contain"
              onError={(e) => {
                console.error("Failed to load logo image");
                e.currentTarget.style.display = "none";
              }}
            />
          </div>
        </div>

        {/* Receipt Content */}
        <div className="p-4">
          <Card className="max-w-sm mx-auto bg-white">
            <CardContent className="p-6">
              <div className="text-center mb-4">
                <div className="border border-gray-300 p-2 mb-4">
                  <div className="text-sm">
                    {t("common.restaurant")}: Phở Đẹp Trai Khanh
                  </div>
                  <div className="text-sm">
                    {t("common.address")}: 서울시 강남구 테헤란로 123
                  </div>
                  <div className="text-sm">
                    {t("common.phone")}: 02-1234-5678
                  </div>
                </div>
                <h2 className="text-lg font-bold">
                  {t("common.invoiceTitle")}
                </h2>
              </div>

              <div className="space-y-2 text-sm mb-4">
                <div className="flex justify-between">
                  <span>{t("orders.orderNumber")}:</span>
                  <span>{selectedOrder.orderNumber}</span>
                </div>
                <div className="flex justify-between">
                  <span>{t("tables.table")}:</span>
                  <span>
                    {selectedOrder.tableId
                      ? `${t("tables.floorLabel")} 1 - ${t("tables.table")} ${selectedOrder.tableId}`
                      : t("common.takeaway")}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("common.time")}:</span>
                  <span>{formatOrderTime(selectedOrder.orderedAt)}</span>
                </div>
                <div className="flex justify-between">
                  <span>{t("common.cashier")}:</span>
                  <span>
                    {selectedOrder.employeeName || t("common.employee")}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("common.customer")}:</span>
                  <span>
                    {selectedOrder.customerName || t("common.walkInCustomer")}
                  </span>
                </div>
              </div>

              <div className="border-t border-b border-gray-300 py-2 mb-4">
                <div className="flex justify-between font-semibold">
                  <span>{t("common.itemName")}</span>
                  <span>{t("common.totalAmount")}</span>
                </div>
              </div>

              <div className="space-y-2 text-sm mb-4">
                {selectedOrderItems && selectedOrderItems.length > 0 ? (
                  selectedOrderItems.map((item: any, index: number) => {
                    // Calculate display price based on priceIncludeTax
                    let displayPrice = parseFloat(item.unitPrice || "0");
                    const priceIncludeTax = selectedOrder.priceIncludeTax === true;
                    const itemTax = parseFloat(item.tax || "0");
                    const quantity = parseInt(item.quantity || "1");
                    
                    if (priceIncludeTax && itemTax > 0) {
                      // If price includes tax: unitPrice = price with tax, need to show price without tax
                      const taxPerUnit = itemTax / quantity;
                      displayPrice = displayPrice - taxPerUnit;
                    }
                    
                    return (
                      <div key={index} className="flex justify-between">
                        <div>
                          <div>{item.productName || "Sản phẩm"}</div>
                          <div className="text-gray-600">
                            {formatCurrency(displayPrice)} x {item.quantity}
                          </div>
                          {item.notes && (
                            <div className="text-xs text-gray-500">
                              {item.notes}
                            </div>
                          )}
                        </div>
                        <div className="text-right">
                          <div className="font-semibold">
                            {formatCurrency(displayPrice * parseInt(item.quantity || "1"))}
                          </div>
                          {parseFloat(item.discount || "0") > 0 && (
                            <div className="text-xs text-red-500">
                              -{formatCurrency(parseFloat(item.discount))}
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="text-center text-gray-500">
                    {isLoading
                      ? t("common.loading")
                      : t("reports.noProductData")}
                  </div>
                )}
              </div>

              <div className="border-t border-gray-300 pt-2 space-y-1 text-sm">
                <div className="flex justify-between">
                  <span>{t("common.subtotal")}</span>
                  <span>
                    {formatCurrency(
                      selectedOrderItems.reduce((sum: number, item: any) => {
                        const displayPrice = parseFloat(item.unitPrice || "0");
                        const quantity = parseInt(item.quantity || "1");
                        return sum + (displayPrice * quantity);
                      }, 0)
                    )}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("common.discount")}</span>
                  <span>
                    {formatCurrency(parseFloat(selectedOrder.discount || "0"))}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("common.tax")}</span>
                  <span>
                    {formatCurrency(parseFloat(selectedOrder.tax || "0"))}
                  </span>
                </div>
                <div className="flex justify-between font-bold">
                  <span>{t("common.totalPayment")}</span>
                  <span>{formatCurrency(parseFloat(selectedOrder.total))}</span>
                </div>
                <div className="flex justify-between text-xs text-gray-600">
                  <span>{t("common.paymentMethod")}:</span>
                  <span>
                    {getPaymentMethodName(
                      selectedOrder.paymentMethod || "cash",
                    )}
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  // Show orders for selected date
  if (selectedDate) {
    const selectedDateData = dailySalesData.find(
      (data) => data.date === selectedDate,
    );

    return (
      <div className="min-h-screen bg-green-50">
        {/* Header */}
        <div className="bg-green-600 text-white p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              className="text-white hover:bg-green-700"
              onClick={() => setSelectedDate(null)}
            >
              <ArrowLeft className="w-5 h-5" />
            </Button>
            <h1 className="text-lg font-semibold">
              {formatDate(selectedDate)}
            </h1>
          </div>
          <div className="flex items-center">
            <img
              src={logoPath}
              alt="EDPOS Logo"
              className="h-8 md:h-12 object-contain"
              onError={(e) => {
                console.error("Failed to load logo image");
                e.currentTarget.style.display = "none";
              }}
            />
          </div>
        </div>

        {/* Orders List */}
        <div className="p-4 space-y-3">
          {selectedDateData?.orders.map((order) => (
            <Card
              key={order.id}
              className="border-0 shadow-sm hover:shadow-md transition-shadow cursor-pointer"
              onClick={() => setSelectedOrder(order)}
            >
              <CardContent className="p-4">
                <div className="flex justify-between items-center">
                  <div className="flex-1">
                    <div className="font-medium text-gray-900">
                      {order.orderNumber}
                    </div>
                    <div className="text-sm text-gray-500">
                      {formatCurrency(parseFloat(order.total))}
                    </div>
                  </div>
                  <ChevronRight className="w-5 h-5 text-gray-400" />
                </div>
              </CardContent>
            </Card>
          ))}

          {/* Total */}
          <Card className="border-0 shadow-sm bg-green-50">
            <CardContent className="p-4">
              <div className="flex justify-between items-center">
                <span className="font-semibold">{t("reports.total")}</span>
                <span className="font-semibold text-green-600">
                  {formatCurrency(selectedDateData?.revenue || 0)}
                </span>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  // Main daily sales list view
  return (
    <div className="min-h-screen bg-green-50">
      {/* Header */}
      <div className="bg-green-600 text-white p-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="icon"
            className="text-white hover:bg-green-700"
            onClick={onBack}
          >
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <h1 className="text-lg font-semibold">{t("reports.dailySales")}</h1>
        </div>
        <div className="flex items-center">
          <img
            src={logoPath}
            alt="EDPOS Logo"
            className="h-8 md:h-12 object-contain"
            onError={(e) => {
              console.error("Failed to load logo image");
              e.currentTarget.style.display = "none";
            }}
          />
        </div>
      </div>

      {/* Filter Bar */}
      <div className="bg-gray-200 p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Calendar className="w-5 h-5 text-gray-600" />
            <Button
              variant="ghost"
              size="sm"
              className="text-sm text-gray-700 hover:bg-gray-300 px-2 py-1"
              onClick={() => setShowDatePicker(!showDatePicker)}
            >
              {t("reports.thisMonth")}
              <ChevronRight
                className={`w-4 h-4 ml-1 transition-transform ${showDatePicker ? "rotate-90" : ""}`}
              />
            </Button>
          </div>
          <div className="flex items-center gap-2">
            {isLoading && (
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 border-2 border-gray-400 border-t-transparent rounded-full animate-spin"></div>
                <span className="text-xs text-gray-600">Đang tải...</span>
              </div>
            )}
            <Filter className="w-4 h-4 text-gray-600" />
          </div>
        </div>

        {/* Date Range Picker */}
        {showDatePicker && (
          <div className="mt-3 p-3 bg-white rounded-lg shadow-sm border">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs text-gray-600 block mb-1">
                  {t("reports.startDate")}
                </label>
                <input
                  type="date"
                  value={dateRange.start}
                  onChange={(e) => {
                    setDateRange((prev) => ({
                      ...prev,
                      start: e.target.value,
                    }));
                    setActiveFilter("custom");
                  }}
                  className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
              <div>
                <label className="text-xs text-gray-600 block mb-1">
                  {t("reports.endDate")}
                </label>
                <input
                  type="date"
                  value={dateRange.end}
                  onChange={(e) => {
                    setDateRange((prev) => ({ ...prev, end: e.target.value }));
                    setActiveFilter("custom");
                  }}
                  className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
            </div>
            <div className="flex gap-2 mt-3">
              <Button
                size="sm"
                onClick={() => {
                  const today = new Date();
                  const start = startOfMonth(today);
                  const end = endOfMonth(today);
                  setDateRange({
                    start: format(start, "yyyy-MM-dd"),
                    end: format(end, "yyyy-MM-dd"),
                  });
                  setActiveFilter("thisMonth");
                }}
                className={`text-xs px-3 py-1 ${
                  activeFilter === "thisMonth"
                    ? "bg-green-600 hover:bg-green-700 text-white"
                    : "border border-green-600 text-green-600 bg-white hover:bg-green-50"
                }`}
              >
                {t("reports.thisMonth")}
              </Button>
              <Button
                size="sm"
                onClick={() => {
                  const today = new Date();
                  const yesterday = subDays(today, 1);
                  setDateRange({
                    start: format(yesterday, "yyyy-MM-dd"),
                    end: format(yesterday, "yyyy-MM-dd"),
                  });
                  setActiveFilter("yesterday");
                }}
                className={`text-xs px-3 py-1 ${
                  activeFilter === "yesterday"
                    ? "bg-green-600 hover:bg-green-700 text-white"
                    : "border border-green-600 text-green-600 bg-white hover:bg-green-50"
                }`}
              >
                {t("reports.lastWeek")}
              </Button>
              <Button
                size="sm"
                onClick={() => {
                  const today = new Date();
                  setDateRange({
                    start: format(today, "yyyy-MM-dd"),
                    end: format(today, "yyyy-MM-dd"),
                  });
                  setActiveFilter("today");
                }}
                className={`text-xs px-3 py-1 ${
                  activeFilter === "today"
                    ? "bg-green-600 hover:bg-green-700 text-white"
                    : "border border-green-600 text-green-600 bg-white hover:bg-green-50"
                }`}
              >
                {t("reports.toDay")}
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Daily Sales List */}
      <div className="p-4 space-y-3">
        {isLoading ? (
          <div className="flex flex-col items-center justify-center py-12 space-y-4">
            <div className="w-8 h-8 border-4 border-green-200 border-t-green-600 rounded-full animate-spin"></div>
            <p className="text-gray-500 text-sm">{t("reports.loadingData")}</p>
          </div>
        ) : (
          <>
            {dailySalesData.map((dailyData) => (
              <Card
                key={dailyData.date}
                className="border-0 shadow-sm hover:shadow-md transition-shadow cursor-pointer"
                onClick={() => setSelectedDate(dailyData.date)}
              >
                <CardContent className="p-4">
                  <div className="flex justify-between items-center">
                    <div className="flex-1">
                      <div className="font-medium text-gray-900">
                        {formatDate(dailyData.date)}
                      </div>
                      <div className="text-sm text-gray-500">
                        {dailyData.orderCount} {t("reports.orders")}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="font-semibold text-green-600">
                        {formatCurrency(dailyData.revenue)}
                      </div>
                      <ChevronRight className="w-5 h-5 text-gray-400" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}

            {dailySalesData.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                {t("reports.noRevenueData")}
              </div>
            )}

            {/* Total */}
            {dailySalesData.length > 0 && (
              <Card className="border-0 shadow-sm bg-green-50">
                <CardContent className="p-4">
                  <div className="flex justify-between items-center">
                    <span className="font-semibold">{t("reports.total")}</span>
                    <span className="font-semibold text-green-600">
                      {formatCurrency(
                        dailySalesData.reduce(
                          (sum, data) => sum + data.revenue,
                          0,
                        ),
                      )}
                    </span>
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </div>
    </div>
  );
}
