import React, { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Calendar,
  DollarSign,
  ShoppingCart,
  Users,
  TrendingUp,
  Package,
  Clock,
  ChartBar,
  ArrowUpRight,
  MoreHorizontal,
  Eye,
} from "lucide-react";
import { useTranslation } from "@/lib/i18n";
import { format, subDays } from "date-fns";
import { vi } from "date-fns/locale";
import { useLocation } from "wouter";

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
  priceIncludeTax?: boolean; // Added for clarity
  tax?: string; // Added for clarity
  discount?: string; // Added for clarity
}

interface OrderItemData {
  id: number;
  orderId: number;
  productId: number;
  productName: string;
  quantity: number;
  unitPrice: string;
  total: string;
}

interface DashboardStats {
  totalSalesRevenue: number;
  subtotalRevenue: number;
  estimatedRevenue: number;
  servingRevenue: number;
  cancelledRevenue: number;
  periodOrderCount: number;
  periodCustomerCount: number;
  dailyAverageRevenue: number;
  activeOrders: number;
  completedOrdersCount: number;
  processingOrdersCount: number;
  cancelledOrdersCount: number;
  totalOrdersInRange: number;
  dateRange: string;
  paymentMethods: { [key: string]: { count: number; total: number } };
  topProducts: Array<{
    name: string;
    quantity: number;
    revenue: number;
    unitPrice: string;
  }>; // Modified to include unitPrice
}

export function DashboardOverview() {
  const { t } = useTranslation();
  const [, setLocation] = useLocation();
  const [dateRange, setDateRange] = useState(() => {
    const today = new Date();
    const formattedToday = format(today, "yyyy-MM-dd");
    return { start: formattedToday, end: formattedToday };
  });

  // Fetch orders
  const { data: ordersData, isLoading: ordersLoading } = useQuery({
    queryKey: ["orders"],
    queryFn: async () => {
      const response = await fetch("https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/orders");
      if (!response.ok) {
        throw new Error("Failed to fetch orders");
      }
      return response.json() as Promise<OrderData[]>;
    },
  });

  // Fetch order items
  const { data: orderItemsData, isLoading: orderItemsLoading } = useQuery({
    queryKey: ["order-items"],
    queryFn: async () => {
      const response = await fetch("https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/order-items");
      if (!response.ok) {
        throw new Error("Failed to fetch order items");
      }
      return response.json() as Promise<OrderItemData[]>;
    },
  });

  // Fetch orders in date range
  const { data: dateRangeOrders, isLoading: dateRangeLoading } = useQuery({
    queryKey: ["orders-date-range", dateRange.start, dateRange.end],
    queryFn: async () => {
      const response = await fetch(
        `https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/orders/date-range/${dateRange.start}/${dateRange.end}`,
      );
      if (!response.ok) {
        throw new Error("Failed to fetch orders in date range");
      }
      return response.json() as Promise<OrderData[]>;
    },
  });

  // Fetch tables for table statistics
  const { data: tablesData } = useQuery({
    queryKey: ["tables"],
    queryFn: async () => {
      const response = await fetch("https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/api/tables");
      if (!response.ok) {
        throw new Error("Failed to fetch tables");
      }
      return response.json();
    },
  });

  const dashboardStats: DashboardStats = useMemo(() => {
    const stats: DashboardStats = {
      totalSalesRevenue: 0,
      subtotalRevenue: 0,
      estimatedRevenue: 0,
      servingRevenue: 0,
      cancelledRevenue: 0,
      periodOrderCount: 0,
      periodCustomerCount: 0,
      dailyAverageRevenue: 0,
      activeOrders: 0,
      completedOrdersCount: 0,
      processingOrdersCount: 0,
      cancelledOrdersCount: 0,
      totalOrdersInRange: 0,
      dateRange: `${dateRange.start} to ${dateRange.end}`,
      paymentMethods: {},
      topProducts: [],
    };

    console.log("Dashboard - Orders loaded:", ordersData?.length || 0);
    console.log("Dashboard - Order items loaded:", orderItemsData?.length || 0);

    if (!ordersData || !dateRangeOrders || !orderItemsData) {
      return stats;
    }

    // Filter completed orders from date range
    const completedOrders = dateRangeOrders.filter(
      (order) => order.status === "completed" || order.status === "paid",
    );

    // Filter serving orders
    const servingOrders = ordersData.filter(
      (order) =>
        order.status === "served" ||
        order.status === "preparing" ||
        order.status === "pending",
    );

    // Filter cancelled orders from date range
    const cancelledOrders = dateRangeOrders.filter(
      (order) => order.status === "cancelled",
    );

    // Count active orders from all orders
    const activeOrdersCount = ordersData.filter(
      (order) =>
        order.status === "pending" ||
        order.status === "preparing" ||
        order.status === "served",
    ).length;

    // Calculate revenues with proper priceIncludeTax handling
    const completedOrdersRevenue = completedOrders.reduce((sum, order) => {
      const orderTotal = parseFloat(order.total || "0");
      const orderTax = parseFloat(order.tax || "0");
      const priceIncludeTax = order.priceIncludeTax === true;

      // If price includes tax, revenue = total - tax
      // If price doesn't include tax, revenue = total (already net of tax)
      const revenue = priceIncludeTax ? orderTotal - orderTax : orderTotal;
      return sum + revenue;
    }, 0);

    const servingOrdersRevenue = servingOrders.reduce((sum, order) => {
      const orderTotal = parseFloat(order.total || "0");
      const orderTax = parseFloat(order.tax || "0");
      const priceIncludeTax = order.priceIncludeTax === true;

      // If price includes tax, revenue = total - tax
      // If price doesn't include tax, revenue = total (already net of tax)
      const revenue = priceIncludeTax ? orderTotal - orderTax : orderTotal;
      return sum + revenue;
    }, 0);

    const cancelledOrdersRevenue = cancelledOrders.reduce((sum, order) => {
      const orderTotal = parseFloat(order.total || "0");
      const orderTax = parseFloat(order.tax || "0");
      const priceIncludeTax = order.priceIncludeTax === true;

      // If price includes tax, revenue = total - tax
      // If price doesn't include tax, revenue = total (already net of tax)
      const revenue = priceIncludeTax ? orderTotal - orderTax : orderTotal;
      return sum + revenue;
    }, 0);

    const estimatedRevenue = completedOrdersRevenue + servingOrdersRevenue;

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

    // Calculate payment methods statistics - total customer payment
    const paymentMethods: { [key: string]: { count: number; total: number } } =
      {};
    completedOrders.forEach((order) => {
      const method = order.paymentMethod || "cash";
      const methodName = getPaymentMethodName(method);
      if (!paymentMethods[methodName]) {
        paymentMethods[methodName] = { count: 0, total: 0 };
      }

      const orderTotal = parseFloat(order.total || "0");
      const orderTax = parseFloat(order.tax || "0");
      const priceIncludeTax = order.priceIncludeTax === true;

      // Calculate revenue first (same as above)
      let revenue;
      if (priceIncludeTax) {
        // If price includes tax: revenue = total - tax
        revenue = orderTotal - orderTax;
      } else {
        // If price doesn't include tax: revenue = total (already net of tax)
        revenue = orderTotal;
      }

      // Customer payment = revenue + tax
      const customerPayment = revenue + orderTax;

      paymentMethods[methodName].count += 1;
      paymentMethods[methodName].total += customerPayment;
    });

    // Get unique customers from date range orders
    const totalCustomers = dateRangeOrders.reduce((total, order) => {
      return total + (order.customerCount || 1);
    }, 0);

    // Calculate top products
    const productStats: {
      [key: string]: {
        quantity: number;
        revenue: number;
        unitPrice: string;
        total: number;
      }; // Added unitPrice
    } = {};

    // Get order items for completed orders in date range
    const completedOrderIds = completedOrders.map((order) => order.id);
    const relevantOrderItems = orderItemsData.filter((item) =>
      completedOrderIds.includes(item.orderId),
    );

    relevantOrderItems.forEach((item) => {
      const productName = item.productName;
      const unitPrice = parseFloat(item.unitPrice || "0");
      const quantity = item.quantity;

      if (!productStats[productName]) {
        productStats[productName] = {
          quantity: 0,
          revenue: 0,
          unitPrice: item.unitPrice,
        };
      }

      // Find the order to get order-level discount
      const order = completedOrders.find((o) => o.id === item.orderId);
      const orderDiscount = parseFloat(order?.discount || "0");

      // Calculate item discount by distributing order discount proportionally
      let itemDiscountAmount = 0;
      if (orderDiscount > 0 && order) {
        // Calculate total before discount for this order
        const orderItems = relevantOrderItems.filter(
          (i) => i.orderId === order.id,
        );
        const totalBeforeDiscount = orderItems.reduce((sum, itm) => {
          return sum + parseFloat(itm.unitPrice || "0") * itm.quantity;
        }, 0);

        // Find if this is the last item in the order
        const currentIndex = orderItems.findIndex((i) => i.id === item.id);
        const isLastItem = currentIndex === orderItems.length - 1;

        if (isLastItem) {
          // Last item: total discount - sum of all previous discounts
          let previousDiscounts = 0;
          for (let i = 0; i < orderItems.length - 1; i++) {
            const prevItem = orderItems[i];
            const prevItemTotal =
              parseFloat(prevItem.unitPrice || "0") * prevItem.quantity;
            const prevItemDiscount =
              totalBeforeDiscount > 0
                ? Math.round(
                    (orderDiscount * prevItemTotal) / totalBeforeDiscount,
                  )
                : 0;
            previousDiscounts += prevItemDiscount;
          }
          itemDiscountAmount = orderDiscount - previousDiscounts;
        } else {
          // Regular calculation for non-last items
          const itemTotal = unitPrice * quantity;
          itemDiscountAmount =
            totalBeforeDiscount > 0
              ? Math.round((orderDiscount * itemTotal) / totalBeforeDiscount)
              : 0;
        }
      }

      // Calculate revenue: price * quantity - distributed discount
      const itemRevenue = unitPrice * quantity - itemDiscountAmount;

      productStats[productName].quantity += quantity;
      productStats[productName].revenue += itemRevenue;
      productStats[productName].unitPrice = item.unitPrice;
    });

    const topProducts = Object.entries(productStats)
      .map(([name, stats]) => ({ name, ...stats }))
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 5);

    // Calculate daily average
    const daysDiff = Math.max(
      1,
      Math.ceil(
        (new Date(dateRange.end).getTime() -
          new Date(dateRange.start).getTime()) /
          (1000 * 60 * 60 * 24),
      ) + 1,
    );

    stats.totalSalesRevenue = completedOrdersRevenue;
    stats.subtotalRevenue = completedOrdersRevenue;
    stats.estimatedRevenue = estimatedRevenue;
    stats.servingRevenue = servingOrdersRevenue;
    stats.cancelledRevenue = cancelledOrdersRevenue;
    stats.periodOrderCount = completedOrders.length;
    stats.periodCustomerCount = totalCustomers;
    stats.dailyAverageRevenue = completedOrdersRevenue / daysDiff;
    stats.activeOrders = activeOrdersCount;
    stats.completedOrdersCount = completedOrders.length;
    stats.processingOrdersCount = servingOrders.length;
    stats.cancelledOrdersCount = cancelledOrders.length;
    stats.totalOrdersInRange = dateRangeOrders.length;
    stats.paymentMethods = paymentMethods;
    stats.topProducts = topProducts;

    console.log("Dashboard Debug - Final Stats:", stats);

    return stats;
  }, [ordersData, dateRangeOrders, orderItemsData, dateRange]);

  const formatCurrency = (amount: number | string) => {
    // Ensure amount is treated as a number, default to 0 if parsing fails
    const numericAmount =
      typeof amount === "string" ? parseFloat(amount) : amount;
    const finalAmount = isNaN(numericAmount) ? 0 : numericAmount;

    return new Intl.NumberFormat("vi-VN", {
      style: "currency",
      currency: "VND",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(finalAmount);
  };

  const getOccupiedTablesCount = () => {
    if (!tablesData) return 0;
    return tablesData.filter((table: any) => table.status === "occupied")
      .length;
  };

  const getTotalTablesCount = () => {
    if (!tablesData) return 10;
    return tablesData.length;
  };

  const handleOrderDetailsClick = () => {
    // Navigate to sales orders page to show order management
    setLocation("/sales-orders");
  };

  if (ordersLoading || orderItemsLoading || dateRangeLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="text-lg text-gray-500">{t("common.loading")}...</div>
      </div>
    );
  }

  return (
    <div className="space-y-4 pb-20">
      {/* Revenue Cards */}
      <div className="grid grid-cols-1 gap-3 px-4">
        {/* Main Revenue Card */}
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="space-y-3">
              <div className="flex justify-between items-start">
                <div>
                  <p className="text-gray-600 text-sm">
                    {t("reports.revenueLabel")}
                  </p>
                  <p className="text-2xl font-bold text-gray-900">
                    {formatCurrency(dashboardStats.totalSalesRevenue)}
                  </p>
                </div>
                <div className="text-green-600 text-sm flex items-center gap-1">
                  <TrendingUp className="w-4 h-4" />
                  {(() => {
                    // Giả sử doanh thu hôm qua (có thể fetch từ API sau)
                    const yesterdayRevenue =
                      dashboardStats.totalSalesRevenue * 0.85; // Mock data
                    const todayRevenue = dashboardStats.totalSalesRevenue;

                    if (yesterdayRevenue === 0) return "+0%";

                    const percentChange =
                      ((todayRevenue - yesterdayRevenue) / yesterdayRevenue) *
                      100;
                    const isPositive = percentChange >= 0;

                    return `${isPositive ? "+" : ""}${percentChange.toFixed(1)}% ${isPositive ? "↑" : "↓"}`;
                  })()}
                </div>
              </div>
              <div className="text-xs text-gray-500">
                {t("reports.comparedToYesterday")}
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Sub Revenue Cards */}
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-gray-600 text-sm">
                  {t("reports.estimatedRevenue")}
                </span>
                <span className="font-semibold">
                  {formatCurrency(dashboardStats.estimatedRevenue)}
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-gray-600 text-sm">
                  {t("reports.paid")}
                </span>
                <span className="font-semibold">
                  {formatCurrency(dashboardStats.totalSalesRevenue)}
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-gray-600 text-sm">
                  {t("reports.serving")}
                </span>
                <span className="font-semibold">
                  {formatCurrency(dashboardStats.servingRevenue)}
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-gray-600 text-sm">
                  {t("reports.cancelled")}
                </span>
                <span className="font-semibold text-red-600">
                  {formatCurrency(dashboardStats.cancelledRevenue)}
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tables and Orders Stats */}
      <div className="grid grid-cols-2 gap-3 px-4">
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="text-center space-y-2">
              <div className="text-sm text-gray-600">
                {t("reports.tablesInUse")}
              </div>
              <div className="text-2xl font-bold text-green-600">
                {getOccupiedTablesCount()}/{getTotalTablesCount()}
              </div>
              <div className="w-12 h-8 bg-gray-200 rounded mx-auto flex items-center justify-center">
                <div className="w-8 h-6 bg-gray-400 rounded"></div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="space-y-2">
              <div className="text-sm text-gray-600">{t("reports.orders")}</div>
              <div className="text-2xl font-bold text-green-600">
                {dashboardStats.totalOrdersInRange}
              </div>
              <div className="space-y-1 text-xs">
                <div className="flex justify-between">
                  <span>{t("reports.completed")}</span>
                  <span className="font-semibold">
                    {dashboardStats.completedOrdersCount}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("reports.processing")}</span>
                  <span className="font-semibold">
                    {dashboardStats.processingOrdersCount}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>{t("reports.cancelled")}</span>
                  <span className="font-semibold">
                    {dashboardStats.cancelledOrdersCount}
                  </span>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Customer Stats */}
      <div className="px-4">
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm text-gray-600">
                  {t("reports.customers")}
                </div>
                <div className="text-2xl font-bold text-green-600">
                  {dashboardStats.periodCustomerCount}
                </div>
                <div className="space-y-1 text-xs mt-2">
                  <div className="flex justify-between">
                    <span>{t("reports.served")}</span>
                    <span className="font-semibold">
                      {Math.floor(dashboardStats.periodCustomerCount * 0.6)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span>{t("reports.currentlyServing")}</span>
                    <span className="font-semibold">
                      {Math.ceil(dashboardStats.periodCustomerCount * 0.4)}
                    </span>
                  </div>
                </div>
              </div>
              <div className="relative">
                <div className="w-16 h-16 rounded-full bg-gray-200 flex items-center justify-center relative">
                  <div
                    className="absolute inset-0 rounded-full border-4 border-green-500"
                    style={{
                      background: `conic-gradient(#10b981 0deg ${
                        Math.min(
                          100,
                          (dashboardStats.periodCustomerCount /
                            Math.max(50, dashboardStats.periodCustomerCount)) *
                            100,
                        ) * 3.6
                      }deg, #e5e7eb ${Math.min(100, (dashboardStats.periodCustomerCount / Math.max(50, dashboardStats.periodCustomerCount)) * 100) * 3.6}deg 360deg)`,
                    }}
                  ></div>
                  <span className="text-sm font-bold text-gray-700">
                    {Math.min(
                      100,
                      Math.round(
                        (dashboardStats.periodCustomerCount /
                          Math.max(50, dashboardStats.periodCustomerCount)) *
                          100,
                      ),
                    )}
                    %
                  </span>
                </div>
                <div className="text-xs text-center mt-1 text-gray-500">
                  {t("reports.serviceCapacity")}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Orders Section */}
      <div className="px-4">
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <div className="flex justify-between items-center mb-3">
              <span className="font-semibold">{t("reports.orders")}</span>
              <button
                onClick={handleOrderDetailsClick}
                className="flex items-center gap-1 text-purple-600 text-sm hover:text-purple-700 transition-colors cursor-pointer"
              >
                <span>{t("reports.viewDetails")}</span>
                <ArrowUpRight className="w-4 h-4" />
              </button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Top 5 Products */}
      <div className="px-4">
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <h3 className="font-semibold mb-4 text-center">
              {t("reports.topSellingProducts")}
            </h3>
            <div className="space-y-3">
              {dashboardStats.topProducts.length > 0 ? (
                dashboardStats.topProducts.map((product, index) => {
                  const colors = [
                    "bg-blue-500",
                    "bg-green-500",
                    "bg-orange-500",
                    "bg-purple-500",
                    "bg-red-500",
                  ];
                  const totalRevenue = dashboardStats.topProducts.reduce(
                    (sum, p) => sum + p.revenue,
                    0,
                  );
                  const percentage =
                    totalRevenue > 0
                      ? ((product.revenue / totalRevenue) * 100).toFixed(0)
                      : "0";

                  return (
                    <div className="flex items-center gap-3" key={index}>
                      <div
                        className={`w-3 h-3 rounded-full ${colors[index] || "bg-gray-500"}`}
                      ></div>
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium truncate">
                          {product.name}
                        </div>
                        <div className="text-xs text-gray-500">
                          {formatCurrency(product.revenue)}
                        </div>
                      </div>
                      <div className="text-xs text-gray-500">{percentage}%</div>
                    </div>
                  );
                })
              ) : (
                <div className="text-sm text-gray-500 text-center">
                  {t("reports.noDataAvailable")}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Payment Methods */}
      <div className="px-4">
        <Card className="border-0 shadow-md">
          <CardContent className="p-4">
            <h3 className="font-semibold mb-4">
              {t("reports.paymentMethods")}
            </h3>

            <div className="space-y-3">
              {Object.entries(dashboardStats.paymentMethods).length > 0 ? (
                Object.entries(dashboardStats.paymentMethods).map(
                  ([method, data], index) => (
                    <div
                      key={index}
                      className="flex justify-between items-center p-3 bg-gray-50 rounded-lg"
                    >
                      <div>
                        <div className="font-medium">{method}</div>
                        <div className="text-sm text-gray-500">
                          {t("reports.orderCount")}:{" "}
                          <span className="font-semibold">
                            {data.count} {t("reports.orders")}
                          </span>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="font-semibold">
                          {t("common.totalCustomerPayment")}:{" "}
                          {formatCurrency(data.total)}
                        </div>
                      </div>
                    </div>
                  ),
                )
              ) : (
                <div className="text-sm text-gray-500 text-center">
                  {t("reports.noPaymentData")}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
